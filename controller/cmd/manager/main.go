// Command manager is the AEI reference operator: it runs the AgentRun reconciler
// and serves the runtime ABI (/complete, /redeem, /vend) so dispatched runtimes
// can call back. It blank-imports the providers it supports; adding a provider is
// a new import, no core change (the database/sql pattern).
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
	ctrlpkg "github.com/jackfrancis/agent-execution-interface/controller/internal/controller"
	"github.com/jackfrancis/agent-execution-interface/controller/internal/dispatch"
	"github.com/jackfrancis/agent-execution-interface/controller/internal/metrics"

	// Register the providers this operator supports.
	_ "github.com/jackfrancis/agent-execution-interface/providers/agentsandbox"
	_ "github.com/jackfrancis/agent-execution-interface/providers/k8sjob"
	_ "github.com/jackfrancis/agent-execution-interface/providers/substrate"
)

func main() {
	var abiAddr string
	flag.StringVar(&abiAddr, "abi-addr", ":8080", "address to serve the runtime ABI (/complete, /redeem, /vend)")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("aei")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aeiv1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		log.Error(err, "unable to build manager")
		os.Exit(1)
	}

	cfg := control.Config{Secret: []byte(os.Getenv("AEI_SIGNING_SECRET"))}
	// An app with an asymmetric identity posture (a sole-minter control tier and a
	// verify-only app tier) provisions the control plane with its Ed25519 private
	// key. When set, the plane mints run credentials with it and the app verifies
	// them with only the public half — no shared secret (e.g. gofer's ADR 0002).
	// Unset keeps the reference-grade HMAC authority.
	if k := strings.TrimSpace(os.Getenv("AEI_ED25519_PRIVATE_KEY")); k != "" {
		raw, decErr := base64.StdEncoding.DecodeString(k)
		if decErr != nil || len(raw) != ed25519.PrivateKeySize {
			log.Error(decErr, "AEI_ED25519_PRIVATE_KEY must be a base64-encoded Ed25519 private key", "wantBytes", ed25519.PrivateKeySize)
			os.Exit(1)
		}
		cfg.Authority = control.NewEd25519Authority(ed25519.PrivateKey(raw))
		log.Info("run-credential authority: Ed25519 (asymmetric, sole-minter)")
	}
	// Route the control plane's run-outcome logs through the manager's logger, so a
	// failed dispatch (e.g. a class whose substrate/CRD is not installed) surfaces
	// in the controller logs instead of only in GET /runs/{id}.
	cfg.Logger = slog.New(logr.ToSlogHandler(ctrl.Log.WithName("control")))
	// Record per-run timing (task, phase, provisioning/runtime/model seconds) into
	// Prometheus, served at /metrics below. This is the platform-supplied metrics
	// seam (control.Observer); the lean control plane keeps no Prometheus import.
	runMetrics := metrics.New()
	cfg.Observer = runMetrics
	plane := control.New(cfg)

	// TokenReview-backed caller identity for the data-plane dispatch API — the
	// app authenticates with its projected ServiceAccount token, no shared secret.
	// When a SPIFFE trust bundle is configured, an offline JWT-SVID authenticator
	// replaces it (no per-dispatch apiserver call; roadmap Phase 6).
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		log.Error(err, "unable to build clientset")
		os.Exit(1)
	}
	disp := dispatch.NewHandler(plane, buildAuthenticator(clientset, log), mgr.GetClient())

	// One server carries both the runtime ABI (/complete, /redeem, /vend) and the
	// data-plane dispatch API (/dispatch, /runs/{id}).
	go func() {
		mux := http.NewServeMux()
		plane.RegisterRoutes(mux)
		disp.RegisterRoutes(mux)
		// Per-run timing and process health for Prometheus to scrape.
		mux.Handle("GET /metrics", runMetrics.Handler())
		srv := &http.Server{Addr: abiAddr, Handler: mux}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error(err, "AEI HTTP server failed")
		}
	}()

	r := &ctrlpkg.AgentRunReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Plane: plane}
	if err := r.SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up reconciler")
		os.Exit(1)
	}

	// Eagerly warm durable apps' actors (a no-op for per-run providers) so the first
	// dispatch does not pay the golden-snapshot cold start.
	appWarmer := &ctrlpkg.AgentAppReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if err := appWarmer.SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up agentapp warming reconciler")
		os.Exit(1)
	}

	log.Info("starting AEI controller", "abiAddr", abiAddr, "providers", aei.Names())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}

// buildAuthenticator selects how the data-plane dispatch API authenticates callers.
// The reference path is a Kubernetes TokenReview: no shared secret, but one apiserver
// call per dispatch. When AEI_SPIFFE_TRUST_DOMAIN, AEI_SPIFFE_AUDIENCE, and
// AEI_SPIFFE_TRUST_BUNDLE_FILE are all set, it uses the OFFLINE SPIFFE JWT-SVID
// authenticator instead — verifying the caller's SVID against the mounted trust
// bundle with no apiserver round-trip (roadmap Phase 6). The bundle file is a JWKS,
// as SPIRE publishes.
func buildAuthenticator(clientset kubernetes.Interface, log logr.Logger) dispatch.Authenticator {
	td := strings.TrimSpace(os.Getenv("AEI_SPIFFE_TRUST_DOMAIN"))
	aud := strings.TrimSpace(os.Getenv("AEI_SPIFFE_AUDIENCE"))
	bundlePath := strings.TrimSpace(os.Getenv("AEI_SPIFFE_TRUST_BUNDLE_FILE"))
	if td == "" || aud == "" || bundlePath == "" {
		return dispatch.NewTokenReviewer(clientset)
	}
	jwks, err := os.ReadFile(bundlePath)
	if err != nil {
		log.Error(err, "AEI_SPIFFE_TRUST_BUNDLE_FILE unreadable", "path", bundlePath)
		os.Exit(1)
	}
	bundle, err := dispatch.ParseTrustBundle(jwks)
	if err != nil {
		log.Error(err, "invalid SPIFFE trust bundle")
		os.Exit(1)
	}
	log.Info("caller identity: offline SPIFFE JWT-SVID (no TokenReview)",
		"trustDomain", td, "audience", aud, "keys", len(bundle))
	return dispatch.NewSPIFFEAuthenticator(td, aud, bundle)
}
