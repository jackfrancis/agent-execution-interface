// Package agentsandbox is an AEI reference provider that dispatches each run as a
// kubernetes-sigs/agent-sandbox Sandbox (agents.x-k8s.io/v1beta1): an isolated,
// single-pod, gVisor/Kata-capable workload the agent-sandbox controller
// reconciles. Like k8s-job it is async: the runtime's /complete callback normally
// ends the run, and Await watches the Sandbox's Finished condition as the backstop
// for the runs that can never call back — a pod that fails to start reports
// PodFailed here and nowhere else, so without the watch such a run could only end
// at its deadline, as a timeout with no reason. It uses the dynamic client (the CRD
// is unstructured) but builds the PodSpec typed via runtimespec, so the runtime
// container + AEI_* injection are byte-identical to every other provider.
package agentsandbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/providers/internal/runtimespec"
)

const (
	providerName      = "agent-sandbox"
	sandboxAPIVersion = "agents.x-k8s.io/v1beta1"
	sandboxKind       = "Sandbox"
	// shutdownGrace is added past a run's deadline for the Sandbox's native
	// scheduled deletion, so a finished Sandbox self-reaps shortly after the run's
	// worst case — the Sandbox equivalent of a Job's TTL.
	shutdownGrace = 5 * time.Minute
	// The upstream Sandbox condition that reports the backing pod reaching a
	// terminal phase, and its two reasons. This is the only place a run that never
	// started is visible, so it is what Await watches.
	conditionFinished = "Finished"
	reasonPodFailed   = "PodFailed"
	// defaultPollInterval paces that watch. A Sandbox is longer-lived than a Job
	// and the callback is the normal completion path, so poll gently.
	defaultPollInterval = 2 * time.Second
)

// sandboxGVR is the agent-sandbox Sandbox resource.
var sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

// init self-registers the provider; a blank import activates "agent-sandbox".
func init() { aei.Register(providerName, build) }

// Options are the dispatch-shaping settings from the provider class parameters.
type Options struct {
	Namespace      string
	Image          string
	ServiceAccount string
	ControlURL     string
	BrokerURL      string
}

func optionsFromConfig(cfg aei.Config) Options {
	ns := cfg.Param("namespace")
	if ns == "" {
		ns = "default"
	}
	return Options{
		Namespace:      ns,
		Image:          cfg.Param("image"),
		ServiceAccount: cfg.Param("serviceAccount"),
		ControlURL:     cfg.Param("controlURL"),
		BrokerURL:      cfg.Param("brokerURL"),
	}
}

// Launcher runs each AEI run as an agent-sandbox Sandbox. It needs create and get
// RBAC on sandboxes: create to dispatch, get so Await can watch for the terminal
// condition. The Sandbox self-reaps via its scheduled deletion.
type Launcher struct {
	client       dynamic.Interface
	kube         kubernetes.Interface
	opts         Options
	pollInterval time.Duration
	now          func() time.Time
	log          *slog.Logger
}

var (
	_ aei.Launcher      = (*Launcher)(nil)
	_ aei.AsyncLauncher = (*Launcher)(nil)
)

// New builds a Launcher over the given dynamic client. build uses it in-cluster;
// tests use it with a dynamic fake.
func New(client dynamic.Interface, opts Options) *Launcher {
	return &Launcher{client: client, opts: opts, pollInterval: defaultPollInterval, now: time.Now}
}

// build constructs the provider from the pod's in-cluster config, failing fast
// outside a cluster.
func build(cfg aei.Config) (aei.Launcher, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("agent-sandbox: not in a cluster: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("agent-sandbox: build client: %w", err)
	}
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("agent-sandbox: build client: %w", err)
	}
	l := New(dyn, optionsFromConfig(cfg))
	l.kube = kube
	l.log = cfg.Logger
	return l, nil
}

// Dispatch creates a Sandbox running the runtime image with the AEI_* injection
// contract and returns a Handle naming it, without waiting.
func (l *Launcher) Dispatch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	// A Sandbox has no isolation field of its own: it carries a PodSpec, so its
	// isolation is that pod's RuntimeClass. Verify it before creating anything.
	if err := runtimespec.VerifyIsolation(ctx, l.kube, spec.Isolation); err != nil {
		return aei.Handle{Provider: providerName}, fmt.Errorf("agent-sandbox: %w", err)
	}
	ns, managed := runtimespec.Namespace(l.opts.Namespace, spec)
	if managed {
		if err := runtimespec.EnsureRuntimeNamespace(ctx, l.kube, ns); err != nil {
			return aei.Handle{Provider: providerName, Namespace: ns}, fmt.Errorf("agent-sandbox: %w", err)
		}
	}
	created, err := l.client.Resource(sandboxGVR).Namespace(ns).
		Create(ctx, l.sandbox(spec, cred, l.now()), metav1.CreateOptions{})
	if err != nil {
		return aei.Handle{Provider: providerName, Namespace: ns}, fmt.Errorf("agent-sandbox: create sandbox: %w", err)
	}
	return aei.Handle{Provider: providerName, Ref: created.GetName(), Namespace: ns}, nil
}

// Await watches the Sandbox named by the Handle to a terminal condition. The
// runtime's /complete callback is still the normal completion path — the control
// plane races this against it, and first outcome wins — but a run whose pod never
// starts (image pull failure, an exhausted node, a crashing sandbox runtime) can
// never call back, and the Sandbox's Finished condition is the only place that is
// visible. Without this the run would just hit its deadline and be reported as a
// timeout, hiding the reason. The per-run deadline (carried on ctx) remains the
// universal backstop behind the watch.
func (l *Launcher) Await(ctx context.Context, h aei.Handle) (aei.Result, error) {
	ns := l.sandboxNamespace(h)
	ticker := time.NewTicker(l.pollInterval)
	defer ticker.Stop()
	for {
		got, err := l.client.Resource(sandboxGVR).Namespace(ns).Get(ctx, h.Ref, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			// The Sandbox is gone (deleted, or reaped) so nothing can report this
			// run's outcome any more. Failing beats waiting out the deadline.
			return aei.Result{Err: fmt.Errorf("agent-sandbox: sandbox %s/%s no longer exists", ns, h.Ref)}, nil
		case err == nil:
			if done, runErr := sandboxTerminal(got); done {
				return aei.Result{Err: runErr}, nil
			}
		}
		// Any other error is treated as transient: retry until the deadline.
		select {
		case <-ctx.Done():
			return aei.Result{Err: ctx.Err()}, nil
		case <-ticker.C:
		}
	}
}

// sandboxTerminal reports whether the Sandbox reached a terminal state, and the
// run error if it failed. It reads the upstream Finished condition, whose reason
// distinguishes a pod that completed from one that failed.
func sandboxTerminal(s *unstructured.Unstructured) (bool, error) {
	conds, found, err := unstructured.NestedSlice(s.Object, "status", "conditions")
	if err != nil || !found {
		return false, nil
	}
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] != conditionFinished || cond["status"] != string(metav1.ConditionTrue) {
			continue
		}
		reason, _ := cond["reason"].(string)
		if reason == reasonPodFailed {
			msg, _ := cond["message"].(string)
			if msg == "" {
				msg = reason
			}
			return true, fmt.Errorf("agent-sandbox: sandbox failed: %s", msg)
		}
		return true, nil // finished without a failure reason: the pod completed
	}
	return false, nil
}

// sandboxNamespace is where Await polls: the namespace Dispatch placed the Sandbox
// in, carried on the Handle. It falls back to resolving the class parameter for a
// hand-built Handle, though the dispatch→await path always sets Handle.Namespace.
func (l *Launcher) sandboxNamespace(h aei.Handle) string {
	if h.Namespace != "" {
		return h.Namespace
	}
	ns, _ := runtimespec.Namespace(l.opts.Namespace, aei.RunSpec{})
	return ns
}

// Launch composes Dispatch and Await for a direct blocking call; an orchestrator
// prefers the split async path so completion is driven by the runtime callback.
func (l *Launcher) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, err := l.Dispatch(ctx, spec, cred)
	if err != nil {
		return h, err
	}
	res, err := l.Await(ctx, h)
	if err != nil {
		return h, err
	}
	return h, res.Err
}

// sandbox builds the Sandbox: the shared runtime PodSpec embedded in
// spec.podTemplate.spec, plus a native scheduled deletion so it self-reaps. Only
// the thin envelope is unstructured; the PodSpec is built typed by runtimespec.
func (l *Launcher) sandbox(spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) *unstructured.Unstructured {
	pod := runtimespec.PodSpec(runtimespec.Options{
		Image:          l.opts.Image,
		ServiceAccount: l.opts.ServiceAccount,
		ControlURL:     l.opts.ControlURL,
		BrokerURL:      l.opts.BrokerURL,
	}, spec, cred, dispatchedAt)
	podMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod)

	ns, _ := runtimespec.Namespace(l.opts.Namespace, spec)
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(sandboxAPIVersion)
	u.SetKind(sandboxKind)
	u.SetName(runtimespec.WorkloadName(spec))
	u.SetNamespace(ns)
	u.SetLabels(runtimespec.Labels(spec))
	_ = unstructured.SetNestedMap(u.Object, podMap, "spec", "podTemplate", "spec")
	_ = unstructured.SetNestedStringMap(u.Object, runtimespec.Labels(spec), "spec", "podTemplate", "metadata", "labels")
	if spec.Deadline > 0 {
		shutdownAt := dispatchedAt.Add(spec.Deadline + shutdownGrace).UTC().Format(time.RFC3339)
		// Both fields sit directly under spec in the Sandbox API. A path the CRD
		// does not define is pruned by the API server without an error, which leaves
		// the default policy (Retain) and no shutdown time: the Sandbox then outlives
		// its run for as long as its container keeps running.
		_ = unstructured.SetNestedField(u.Object, shutdownAt, "spec", "shutdownTime")
		_ = unstructured.SetNestedField(u.Object, "Delete", "spec", "shutdownPolicy")
	}
	return u
}
