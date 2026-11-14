// Package k8sjob is an AEI reference provider that dispatches each run as a
// Kubernetes batch/v1 Job: an in-cluster, one-shot, watched, push-token substrate.
// It is the simplest provider — the bracket against which richer ones (isolated,
// remote, durable) are compared — and lives in the providers module so its
// client-go dependency never enters an app's graph that only imports the aei core.
//
// It self-registers as "k8s-job"; activate it with a blank import of this package
// and select it by name (aei.Build("k8s-job", cfg)) or, under the CRD surface, via
// an AgentProviderClass bound to it.
package k8sjob

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/providers/internal/runtimespec"
)

const (
	providerName = "k8s-job"
	// defaultTTLAfterFinished self-reaps a finished Job (the run reports its own
	// outcome via the callback, so the Job object is only kept briefly for
	// observability).
	defaultTTLAfterFinished int32 = 900
	defaultPollInterval           = 2 * time.Second
)

// init self-registers the provider (the database/sql pattern). A blank import of
// this package activates "k8s-job"; adding a provider touches no core wiring.
func init() { aei.Register(providerName, build) }

// Options are the dispatch-shaping settings, sourced from the provider class
// parameters (aei.Config.Params): the runtime image, its namespace and service
// account, and the AEI control/broker URLs the runtime calls back.
type Options struct {
	Namespace      string
	Image          string
	ServiceAccount string
	ControlURL     string
	BrokerURL      string
	// Tolerations go on every run's pod; see runtimespec.ParseTolerations.
	Tolerations []corev1.Toleration
}

// optionsFromConfig reads the provider class parameters. It fails on a malformed
// "tolerations" value, so a typo stops the class from loading instead of leaving
// its runs Pending.
func optionsFromConfig(cfg aei.Config) (Options, error) {
	tolerations, err := runtimespec.ParseTolerations(cfg.Param("tolerations"))
	if err != nil {
		return Options{}, fmt.Errorf("k8s-job: parameter tolerations: %w", err)
	}
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
		Tolerations:    tolerations,
	}, nil
}

// Launcher runs each AEI run as a Kubernetes Job. It is an AsyncLauncher: Dispatch
// creates the Job without waiting, and Await watches it to completion, so a slow
// run never pins a dispatch worker and completion can be raced against the
// runtime's callback.
type Launcher struct {
	client       kubernetes.Interface
	opts         Options
	pollInterval time.Duration
	log          *slog.Logger
	now          func() time.Time
}

var (
	_ aei.Launcher      = (*Launcher)(nil)
	_ aei.AsyncLauncher = (*Launcher)(nil)
)

// New builds a Launcher over the given client. build uses it with an in-cluster
// client; tests use it with a fake.
func New(client kubernetes.Interface, opts Options) *Launcher {
	return &Launcher{client: client, opts: opts, pollInterval: defaultPollInterval, now: time.Now}
}

// build constructs the provider from the pod's in-cluster config. Like every
// Kubernetes provider it only works inside a cluster, failing fast otherwise so
// selection surfaces the misconfiguration rather than silently degrading.
func build(cfg aei.Config) (aei.Launcher, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s-job: not in a cluster: %w", err)
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s-job: build client: %w", err)
	}
	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	l := New(cs, opts)
	l.log = cfg.Logger
	return l, nil
}

// Dispatch creates a Job that runs the runtime image with the AEI_* injection
// contract and returns a Handle naming it, without waiting (the async split).
func (l *Launcher) Dispatch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	// Isolation first: an unsatisfiable class must fail the run, not downgrade it.
	if err := runtimespec.VerifyIsolation(ctx, l.client, spec.Isolation); err != nil {
		return aei.Handle{Provider: providerName}, fmt.Errorf("k8s-job: %w", err)
	}
	ns, managed := runtimespec.Namespace(l.opts.Namespace, spec)
	if managed {
		if err := runtimespec.EnsureRuntimeNamespace(ctx, l.client, ns); err != nil {
			return aei.Handle{Provider: providerName, Namespace: ns}, fmt.Errorf("k8s-job: %w", err)
		}
	}
	created, err := l.client.BatchV1().Jobs(ns).
		Create(ctx, l.job(spec, cred, l.now()), metav1.CreateOptions{})
	if err != nil {
		return aei.Handle{Provider: providerName, Namespace: ns}, fmt.Errorf("k8s-job: create job: %w", err)
	}
	return aei.Handle{Provider: providerName, Ref: created.Name, Namespace: ns}, nil
}

// Await watches the Job named by the Handle to a terminal condition. It keys off
// the Handle alone, so it can run on its own goroutine with no shared state. The
// per-run deadline (carried on ctx) is the universal backstop: on ctx done the
// run's terminal outcome is a timeout, reported as Result.Err.
func (l *Launcher) Await(ctx context.Context, h aei.Handle) (aei.Result, error) {
	ns := l.jobNamespace(h)
	ticker := time.NewTicker(l.pollInterval)
	defer ticker.Stop()
	for {
		if job, err := l.client.BatchV1().Jobs(ns).Get(ctx, h.Ref, metav1.GetOptions{}); err == nil {
			if done, runErr := jobTerminal(job); done {
				return aei.Result{Err: runErr}, nil
			}
		}
		select {
		case <-ctx.Done():
			return aei.Result{Err: ctx.Err()}, nil
		case <-ticker.C:
		}
	}
}

// jobNamespace is where Await polls: the namespace Dispatch placed the Job in,
// carried on the Handle. It falls back to resolving the class parameter for a
// hand-built Handle, though the dispatch→await path always sets Handle.Namespace.
func (l *Launcher) jobNamespace(h aei.Handle) string {
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

// job builds the one-shot Job: no restart, no backoff (the runtime reports its
// own outcome), a TTL so a finished Job self-reaps, and the run deadline as the
// Job's activeDeadlineSeconds. The single runtime container carries the AEI_*
// injection contract built by aei.Injection — identical to every other provider.
func (l *Launcher) job(spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) *batchv1.Job {
	backoff := int32(0)
	ttl := defaultTTLAfterFinished
	var activeDeadline *int64
	if spec.Deadline > 0 {
		s := int64(spec.Deadline.Seconds())
		activeDeadline = &s
	}
	ns, _ := runtimespec.Namespace(l.opts.Namespace, spec)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runtimespec.WorkloadName(spec),
			Namespace: ns,
			Labels:    runtimespec.Labels(spec),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: runtimespec.Labels(spec)},
				Spec:       runtimespec.PodSpec(l.runtimeOptions(), spec, cred, dispatchedAt),
			},
		},
	}
}

// runtimeOptions maps this provider's config to the shared PodSpec-shaping options.
func (l *Launcher) runtimeOptions() runtimespec.Options {
	return runtimespec.Options{
		Image:          l.opts.Image,
		ServiceAccount: l.opts.ServiceAccount,
		ControlURL:     l.opts.ControlURL,
		BrokerURL:      l.opts.BrokerURL,
		Tolerations:    l.opts.Tolerations,
	}
}

// jobTerminal reports whether the Job has reached a terminal condition, and the
// run outcome: nil on Complete, an error on Failed.
func jobTerminal(job *batchv1.Job) (bool, error) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, nil
		case batchv1.JobFailed:
			return true, fmt.Errorf("k8s-job: run failed: %s", c.Reason)
		}
	}
	return false, nil
}
