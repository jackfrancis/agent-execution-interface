// Package controller holds the AEI reference reconciler: it implements the
// declarative surface (AgentRun / AgentProviderClass) by resolving the class to a
// provider and driving the run through the very same control.Plane an embedded Go
// app uses. The CRD surface is therefore a thin projection, not a second engine —
// which is exactly the design's intent (docs/design.md, the declarative surface).
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=agentruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=agentruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=agentproviderclasses,verbs=get;list;watch

// AgentRunReconciler reconciles AgentRun objects. It holds one shared control
// plane (broker + completion registry + ABI server) and resolves a provider per
// AgentProviderClass, dispatching through Plane.Run.
type AgentRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Plane is the shared control plane; the manager serves Plane.Handler() so
	// runtimes can call /complete, /redeem, /vend.
	Plane *control.Plane

	mu        sync.Mutex
	inflight  map[types.UID]bool
	launchers map[string]aei.Launcher // resolved provider per class name
}

// Reconcile drives an AgentRun to a terminal phase: resolve its class, build the
// provider, dispatch, and (asynchronously) record the outcome. It is level-based
// and dispatches each run exactly once (an in-memory guard; reference-grade,
// single-replica).
func (r *AgentRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run aeiv1.AgentRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if run.Status.Phase == aeiv1.PhaseSucceeded || run.Status.Phase == aeiv1.PhaseFailed {
		return ctrl.Result{}, nil
	}
	if !r.claim(run.UID) {
		return ctrl.Result{}, nil // a dispatch for this run is already in flight
	}

	class, err := r.resolveClass(ctx, run.Spec.ProviderClassName)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &run, "ClassResolutionFailed", err)
	}
	// Admit isolation before anything is dispatched: a run that cannot get the
	// isolation it names must fail, not run under the default runtime.
	if err := class.CheckIsolation(class.ResolveIsolation(run.Spec.IsolationClass), run.Spec.Requirements.Isolation); err != nil {
		return ctrl.Result{}, r.fail(ctx, &run, "IsolationNotSatisfied", err)
	}
	launcher, err := r.launcherFor(class)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &run, "ProviderBuildFailed", err)
	}

	run.Status.Phase = aeiv1.PhaseRunning
	run.Status.Handle = aeiv1.HandleStatus{Provider: class.Spec.Provider, Ref: string(run.UID)}
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: "Dispatched", Status: metav1.ConditionTrue, Reason: "Dispatched",
		Message: "dispatched to provider " + class.Spec.Provider, ObservedGeneration: run.Generation,
	})
	if err := r.Status().Update(ctx, &run); err != nil {
		r.release(run.UID) // let a later reconcile retry the dispatch
		return ctrl.Result{}, err
	}

	go r.runAndReport(req.NamespacedName, launcher, toRunSpec(&run, class))
	return ctrl.Result{}, nil
}

// runAndReport drives the run to completion through the shared plane (blocking:
// it races the callback, the watch, and the deadline) and records the terminal
// outcome on the AgentRun's status, retrying on write conflicts.
func (r *AgentRunReconciler) runAndReport(nn types.NamespacedName, launcher aei.Launcher, spec aei.RunSpec) {
	res, _ := r.Plane.Run(context.Background(), launcher, spec)

	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var run aeiv1.AgentRun
		if err := r.Get(context.Background(), nn, &run); err != nil {
			return client.IgnoreNotFound(err)
		}
		if run.Status.Phase == aeiv1.PhaseSucceeded || run.Status.Phase == aeiv1.PhaseFailed {
			return nil // already finalized
		}
		reason := "CallbackReceived"
		if res.Err != nil {
			run.Status.Phase, reason = aeiv1.PhaseFailed, "RunFailed"
		} else {
			run.Status.Phase = aeiv1.PhaseSucceeded
		}
		run.Status.ResultRef = res.ResultRef
		run.Status.Timing = toTimingStatus(res.Timing)
		cond := metav1.Condition{Type: "Finished", Status: metav1.ConditionTrue, Reason: reason, ObservedGeneration: run.Generation}
		if res.Err != nil {
			cond.Message = res.Err.Error()
		}
		apimeta.SetStatusCondition(&run.Status.Conditions, cond)
		return r.Status().Update(context.Background(), &run)
	})
}

// resolveClass fetches the named AgentProviderClass, or the cluster default when
// the name is empty.
func (r *AgentRunReconciler) resolveClass(ctx context.Context, name string) (*aeiv1.AgentProviderClass, error) {
	if name != "" {
		var c aeiv1.AgentProviderClass
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &c); err != nil {
			return nil, fmt.Errorf("provider class %q: %w", name, err)
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
	return nil, fmt.Errorf("no providerClassName set and no default AgentProviderClass")
}

// launcherFor builds (and caches) the provider bound to a class.
func (r *AgentRunReconciler) launcherFor(class *aeiv1.AgentProviderClass) (aei.Launcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.launchers[class.Name]; ok {
		return l, nil
	}
	l, err := aei.Build(class.Spec.Provider, aei.Config{Params: class.Spec.Parameters})
	if err != nil {
		return nil, err
	}
	r.launchers[class.Name] = l
	return l, nil
}

func (r *AgentRunReconciler) fail(ctx context.Context, run *aeiv1.AgentRun, reason string, cause error) error {
	run.Status.Phase = aeiv1.PhaseFailed
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: "Finished", Status: metav1.ConditionTrue, Reason: reason,
		Message: cause.Error(), ObservedGeneration: run.Generation,
	})
	return r.Status().Update(ctx, run)
}

func (r *AgentRunReconciler) claim(uid types.UID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inflight[uid] {
		return false
	}
	r.inflight[uid] = true
	return true
}

func (r *AgentRunReconciler) release(uid types.UID) {
	r.mu.Lock()
	delete(r.inflight, uid)
	r.mu.Unlock()
}

// SetupWithManager registers the reconciler and initializes its in-memory state.
func (r *AgentRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.inflight == nil {
		r.inflight = map[types.UID]bool{}
	}
	if r.launchers == nil {
		r.launchers = map[string]aei.Launcher{}
	}
	return ctrl.NewControllerManagedBy(mgr).For(&aeiv1.AgentRun{}).Named("agentrun").Complete(r)
}

func toRunSpec(run *aeiv1.AgentRun, class *aeiv1.AgentProviderClass) aei.RunSpec {
	return aei.RunSpec{
		ID:         string(run.UID),
		TaskRef:    run.Spec.TaskRef,
		Image:      run.Spec.Image,
		Parameters: run.Spec.Parameters,
		Isolation:  class.ResolveIsolation(run.Spec.IsolationClass),
		Identity: aei.IdentityRequest{
			Subject:  run.Spec.Identity.Subject,
			Scopes:   run.Spec.Identity.Scopes,
			Audience: run.Spec.Identity.Audience,
		},
		Deadline:     time.Duration(run.Spec.TimeoutSeconds) * time.Second,
		AppNamespace: run.Namespace,
		Session:      run.Spec.Session,
		Metadata: map[string]string{
			"agents.x-k8s.io/namespace": run.Namespace,
			"agents.x-k8s.io/name":      run.Name,
		},
	}
}

func toTimingStatus(t aei.Timing) aeiv1.TimingStatus {
	return aeiv1.TimingStatus{
		ProvisioningSeconds: t.ProvisioningSeconds,
		RuntimeSeconds:      t.RuntimeSeconds,
		ModelSeconds:        t.ModelSeconds,
		ModelCalls:          t.ModelCalls,
		ToolCalls:           t.ToolCalls,
	}
}
