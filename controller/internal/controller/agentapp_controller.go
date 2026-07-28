package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/jackfrancis/agent-execution-interface/aei"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// warmRequeue is how long the warming reconciler waits before retrying when the class
// is not yet present/configured or the provider's platform is still coming up.
const warmRequeue = 30 * time.Second

// AgentAppReconciler eagerly warms an app's durable infrastructure. When an AgentApp
// binds a provider class whose launcher is an aei.Warmer (the durable agent-substrate
// provider), it materializes the actor + golden snapshot ahead of the first run, so
// the first dispatch does not pay the cold start. For providers that create a per-run
// workload (no Warmer — the k8s-job and agent-sandbox providers) it is a no-op. It is
// purely an optimization: warming is idempotent, and a run still provisions lazily if
// it races ahead of warming.
//
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=agentapps,verbs=get;list;watch
type AgentAppReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// build resolves the provider launcher for a class; nil uses the registry
	// (aei.Build), same as the run reconciler. Overridable in tests.
	build func(*aeiv1.AgentProviderClass) (aei.Launcher, error)

	mu        sync.Mutex
	launchers map[string]aei.Launcher
}

// Reconcile warms the app's durable actor when its class's provider supports warming;
// otherwise it is a no-op. Failures requeue with backoff rather than failing the app —
// warming is best-effort, and the dispatch path still provisions lazily.
func (r *AgentAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var app aeiv1.AgentApp
	if err := r.Get(ctx, req.NamespacedName, &app); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A durable actor is materialized from the app's image; nothing to warm without one.
	if app.Spec.Image == "" {
		return ctrl.Result{}, nil
	}
	class, err := r.resolveClass(ctx, app.Spec.ProviderClassName)
	if err != nil {
		// The class is often applied after the app; retry rather than fail.
		return ctrl.Result{RequeueAfter: warmRequeue}, nil
	}
	launcher, err := r.launcherFor(class)
	if err != nil {
		return ctrl.Result{RequeueAfter: warmRequeue}, nil
	}
	warmer, ok := launcher.(aei.Warmer)
	if !ok {
		return ctrl.Result{}, nil // a per-run provider: nothing to pre-provision
	}
	spec := aei.RunSpec{App: app.Name, AppNamespace: app.Namespace, Image: app.Spec.Image}
	if err := warmer.Warm(ctx, spec); err != nil {
		// Transient (e.g. the substrate platform or its RBAC is not ready yet); retry.
		log.FromContext(ctx).Info("agent app warm deferred",
			"app", app.Namespace+"/"+app.Name, "class", class.Name, "err", err.Error())
		return ctrl.Result{RequeueAfter: warmRequeue}, nil
	}
	return ctrl.Result{}, nil
}

// resolveClass gets the named class, or the cluster default when the app names none.
func (r *AgentAppReconciler) resolveClass(ctx context.Context, name string) (*aeiv1.AgentProviderClass, error) {
	if name != "" {
		var c aeiv1.AgentProviderClass
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &c); err != nil {
			return nil, err
		}
		return &c, nil
	}
	var list aeiv1.AgentProviderClassList
	if err := r.List(ctx, &list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Annotations[aeiv1.IsDefaultClassAnnotation] == "true" {
			return &list.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no providerClassName and no default AgentProviderClass")
}

// launcherFor builds (and caches) the provider bound to a class.
func (r *AgentAppReconciler) launcherFor(class *aeiv1.AgentProviderClass) (aei.Launcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.launchers == nil {
		r.launchers = map[string]aei.Launcher{}
	}
	if l, ok := r.launchers[class.Name]; ok {
		return l, nil
	}
	build := r.build
	if build == nil {
		build = func(c *aeiv1.AgentProviderClass) (aei.Launcher, error) {
			return aei.Build(c.Spec.Provider, aei.Config{Params: c.Spec.Parameters})
		}
	}
	l, err := build(class)
	if err != nil {
		return nil, err
	}
	r.launchers[class.Name] = l
	return l, nil
}

// SetupWithManager registers the warming reconciler on AgentApp events.
func (r *AgentAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&aeiv1.AgentApp{}).Named("agentapp-warm").Complete(r)
}
