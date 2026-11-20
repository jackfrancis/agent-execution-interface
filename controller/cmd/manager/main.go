// Command manager is the AEI reference operator: it runs the AgentRun reconciler
// and serves the runtime ABI (/complete, /redeem, /vend) so dispatched runtimes
// can call back, and the dispatch API on a listener of its own. It blank-imports
// the providers it supports; adding a provider is a new import, no core change
// (the database/sql pattern).
package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"
	"time"

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
	var abiAddr, dispatchAddr, dispatchAudience string
	var runDefaultTimeout, runMaxTimeout time.Duration
	flag.StringVar(&abiAddr, "abi-addr", ":8080", "address to serve the runtime ABI (/complete, /redeem, /vend, /authority) and /metrics")
	flag.StringVar(&dispatchAddr, "dispatch-addr", "", "address to serve the dispatch API (/dispatch, /runs/{id}) on its own listener, so a NetworkPolicy can restrict it by port (default: the -abi-addr host on port 8081)")
	flag.StringVar(&dispatchAudience, "dispatch-audience", dispatch.DefaultAudience, "token audience the dispatch API requires (its TokenReview requests it); callers project a ServiceAccount token for it")
	var maxCreates, maxQueued int
	flag.IntVar(&maxCreates, "max-concurrent-creates", control.DefaultMaxConcurrentCreates, "maximum provider creates (Dispatch/Launch) in progress at once, across all providers")
	flag.IntVar(&maxQueued, "max-queued-runs", control.DefaultMaxQueued, "maximum dispatched runs waiting for a create slot; beyond it /dispatch answers 429")
	var createTimeout time.Duration
	flag.DurationVar(&createTimeout, "create-timeout", control.DefaultCreateTimeout, "maximum time minting a run credential plus an async provider's Dispatch may hold a create slot; the create is abandoned and the run fails at it")
	flag.DurationVar(&runDefaultTimeout, "run-default-timeout", 0, "deadline for a run dispatched without a timeout (0 = 15m)")
	flag.DurationVar(&runMaxTimeout, "run-max-timeout", 0, "longest deadline a run may have; a longer timeout is reduced to it (0 = 1h)")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("aei")

	// Fail closed: refuse to start without a usable signing key, so no run is ever
	// dispatched with an empty or forgeable credential. An app with an asymmetric
	// identity posture (a sole-minter control tier and a verify-only app tier, e.g.
	// gofer's ADR 0002) gets the Ed25519 key 'make authority' provisions.
	cfg := control.Config{
		MaxConcurrentCreates: maxCreates,
		MaxQueued:            maxQueued,
		CreateTimeout:        createTimeout,
		// Every run gets a deadline the controller enforces itself: a dispatch or
		// AgentRun without a timeout gets the default, and a longer one is capped.
		DefaultDeadline: runDefaultTimeout,
		MaxDeadline:     runMaxTimeout,
	}
	kind, err := configureSigning(&cfg, os.Getenv)
	if err != nil {
		log.Error(err, "refusing to start")
		os.Exit(1)
	}
	log.Info("run-credential authority: " + kind)

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
	auth := buildAuthenticator(clientset, dispatchAudience, log)
	disp := dispatch.NewHandler(plane, auth, mgr.GetClient())

	// Two listeners, so a NetworkPolicy (which matches ports, not paths) can admit
	// runtime pods and Prometheus to the ABI while only apps reach dispatch. The
	// manager runs both, so they stop with the controllers on SIGTERM and drain
	// in-flight requests first.
	dispatchAddr, err = resolveDispatchAddr(abiAddr, dispatchAddr)
	if err != nil {
		log.Error(err, "invalid listener addresses")
		os.Exit(1)
	}
	abiMux, dispatchMux := newMuxes(plane, disp, runMetrics.Handler())
	for _, s := range []*httpServer{
		{name: "abi", addr: abiAddr, handler: abiMux},
		{name: "dispatch", addr: dispatchAddr, handler: dispatchMux},
	} {
		s.timeouts, s.log = defaultServerTimeouts, log
		if err := mgr.Add(s); err != nil {
			log.Error(err, "unable to add AEI HTTP server", "server", s.name)
			os.Exit(1)
		}
	}

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

	log.Info("starting AEI controller", "abiAddr", abiAddr, "dispatchAddr", dispatchAddr,
		"dispatchAudience", dispatchAudience, "providers", aei.Names())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}

// buildAuthenticator selects how the data-plane dispatch API authenticates callers.
// The reference path is a Kubernetes TokenReview for the dispatch audience: no shared
// secret, but one apiserver call per dispatch. When AEI_SPIFFE_TRUST_DOMAIN, AEI_SPIFFE_AUDIENCE, and
// AEI_SPIFFE_TRUST_BUNDLE_FILE are all set, it uses the OFFLINE SPIFFE JWT-SVID
// authenticator instead — verifying the caller's SVID against the mounted trust
// bundle with no apiserver round-trip (roadmap Phase 6). The bundle file is a JWKS,
// as SPIRE publishes.
func buildAuthenticator(clientset kubernetes.Interface, audience string, log logr.Logger) dispatch.Authenticator {
	td := strings.TrimSpace(os.Getenv("AEI_SPIFFE_TRUST_DOMAIN"))
	aud := strings.TrimSpace(os.Getenv("AEI_SPIFFE_AUDIENCE"))
	bundlePath := strings.TrimSpace(os.Getenv("AEI_SPIFFE_TRUST_BUNDLE_FILE"))
	if td == "" || aud == "" || bundlePath == "" {
		return dispatch.NewTokenReviewer(clientset, audience)
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
