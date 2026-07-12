// Command manager is the AEI reference operator: it runs the AgentRun reconciler
// and serves the runtime ABI (/complete, /redeem, /vend) so dispatched runtimes
// can call back. It blank-imports the providers it supports; adding a provider is
// a new import, no core change (the database/sql pattern).
package main

import (
	"flag"
	"net/http"
	"os"

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

	plane := control.New(control.Config{Secret: []byte(os.Getenv("AEI_SIGNING_SECRET"))})

	// TokenReview-backed caller identity for the data-plane dispatch API — the
	// app authenticates with its projected ServiceAccount token, no shared secret.
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		log.Error(err, "unable to build clientset")
		os.Exit(1)
	}
	disp := dispatch.NewHandler(plane, dispatch.NewTokenReviewer(clientset), mgr.GetClient())

	// One server carries both the runtime ABI (/complete, /redeem, /vend) and the
	// data-plane dispatch API (/dispatch, /runs/{id}).
	go func() {
		mux := http.NewServeMux()
		plane.RegisterRoutes(mux)
		disp.RegisterRoutes(mux)
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

	log.Info("starting AEI controller", "abiAddr", abiAddr, "providers", aei.Names())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}
