// Package agentsandbox is an AEI reference provider that dispatches each run as a
// kubernetes-sigs/agent-sandbox Sandbox (agents.x-k8s.io/v1beta1): an isolated,
// single-pod, gVisor/Kata-capable workload the agent-sandbox controller
// reconciles. It is the isolated, callback-driven bracket opposite k8s-job: a
// Sandbox has no batch-style completion, so Await is DETACHED — it waits the
// deadline while the real outcome arrives via the runtime's /complete callback,
// which the control plane races against it. It uses the dynamic client (the CRD is
// unstructured) but builds the PodSpec typed via runtimespec, so the runtime
// container + AEI_* injection are byte-identical to every other provider.
package agentsandbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
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

// Launcher runs each AEI run as an agent-sandbox Sandbox. It is detached and needs
// only create RBAC on sandboxes — the Sandbox self-reaps via its scheduled deletion.
type Launcher struct {
	client dynamic.Interface
	opts   Options
	now    func() time.Time
	log    *slog.Logger
}

var (
	_ aei.Launcher      = (*Launcher)(nil)
	_ aei.AsyncLauncher = (*Launcher)(nil)
)

// New builds a Launcher over the given dynamic client. build uses it in-cluster;
// tests use it with a dynamic fake.
func New(client dynamic.Interface, opts Options) *Launcher {
	return &Launcher{client: client, opts: opts, now: time.Now}
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
	l := New(dyn, optionsFromConfig(cfg))
	l.log = cfg.Logger
	return l, nil
}

// Dispatch creates a Sandbox running the runtime image with the AEI_* injection
// contract and returns a Handle naming it, without waiting.
func (l *Launcher) Dispatch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	created, err := l.client.Resource(sandboxGVR).Namespace(l.opts.Namespace).
		Create(ctx, l.sandbox(spec, cred, l.now()), metav1.CreateOptions{})
	if err != nil {
		return aei.Handle{Provider: providerName}, fmt.Errorf("agent-sandbox: create sandbox: %w", err)
	}
	return aei.Handle{Provider: providerName, Ref: created.GetName()}, nil
}

// Await is detached: a Sandbox has no batch-style completion, so it waits the
// per-run deadline (carried on ctx). The real outcome arrives via the runtime's
// callback, which the control plane races against this; reaching the deadline
// means no report arrived in time (a timeout).
func (l *Launcher) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
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

	u := &unstructured.Unstructured{}
	u.SetAPIVersion(sandboxAPIVersion)
	u.SetKind(sandboxKind)
	u.SetName(runtimespec.Name("aei", spec.ID))
	u.SetNamespace(l.opts.Namespace)
	u.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "aei"})
	_ = unstructured.SetNestedMap(u.Object, podMap, "spec", "podTemplate", "spec")
	if spec.Deadline > 0 {
		shutdownAt := dispatchedAt.Add(spec.Deadline + shutdownGrace).UTC().Format(time.RFC3339)
		_ = unstructured.SetNestedField(u.Object, shutdownAt, "spec", "lifecycle", "shutdownTime")
		_ = unstructured.SetNestedField(u.Object, "Delete", "spec", "lifecycle", "shutdownPolicy")
	}
	return u
}
