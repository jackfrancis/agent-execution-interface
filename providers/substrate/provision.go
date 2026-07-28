package substrate

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// The ate.dev CRDs AEI materializes for a durable actor. Actor and Atespace are
// NOT CRDs — they are ate-api gRPC objects (see ateclient.go), so they cannot be
// applied with the dynamic client and go through the actorEnsurer seam instead.
var (
	actorTemplateGVR = schema.GroupVersionResource{Group: "ate.dev", Version: "v1alpha1", Resource: "actortemplates"}
	workerPoolGVR    = schema.GroupVersionResource{Group: "ate.dev", Version: "v1alpha1", Resource: "workerpools"}
)

const (
	// workerPoolLabel ties an ActorTemplate to its WorkerPool for scheduling. ate-api's
	// syncer gives each Worker the eligibility labels of its WorkerPool's OWN metadata
	// (pool.GetLabels()) — NOT the worker pod's labels — and matches the ActorTemplate's
	// workerSelector against them. AEI therefore stamps this label on BOTH the WorkerPool
	// metadata and the workerSelector; dropping it from either (or a key mismatch) yields
	// the dispatch error "no free workers available". The key is AEI's own choice: it
	// reuses the ate.dev/worker-pool key the ate controller puts on the pods, but since
	// eligibility reads the WorkerPool metadata, any consistent key would work.
	workerPoolLabel = "ate.dev/worker-pool"
	managedByKey    = "app.kubernetes.io/managed-by" // marks AEI-created ate objects
	managedByVal    = "aei"
)

// actorEnsurer creates the ate-api objects an actor needs — its atespace and the
// actor itself — which are not Kubernetes CRDs. It is an interface so the gRPC
// client is an isolated, swappable dependency the provider fakes in tests. Both
// calls must be idempotent (a re-dispatch re-ensures).
type actorEnsurer interface {
	EnsureAtespace(ctx context.Context, name string) error
	EnsureActor(ctx context.Context, atespace, name, templateNamespace, templateName string) error
}

// ensure materializes the durable actor for a run before dispatch, so an app bound
// to a durable class ships no ate artifacts — only its image and an AgentApp. It is
// idempotent and runs at most once per actor identity: it applies the WorkerPool
// (only when an ateomImage is configured) and the ActorTemplate (composed from the
// app image + admin class params), then creates the atespace and actor via the
// ate-api. It is a no-op when auto-provisioning is off (no cluster client, or an
// actorHost override that means the admin fully owns the actor).
func (l *Launcher) ensure(ctx context.Context, spec aei.RunSpec) error {
	if l.dyn == nil || l.opts.ActorHost != "" {
		l.warnUnprovisionableImage(spec)
		return nil
	}
	actor, atespace := l.opts.resolveActor(spec)
	key := atespace + "/" + actor
	if _, done := l.ensured.Load(key); done {
		return nil
	}

	// AEI provisions the whole chain (WorkerPool + ActorTemplate + atespace + actor)
	// or nothing: the actor references its template, so creating the actor without a
	// template AEI also created fails with "ActorTemplate not found". It manages the
	// chain only when it has what it needs — an app image to run and a snapshot location
	// for the template. Without either, the actor is assumed pre-provisioned out of
	// band, so AEI provisions nothing and the dispatch either reaches a standing actor
	// or fails at the router (a clearer signal than a half-provisioned chain).
	if spec.Image == "" || l.opts.SnapshotLocation == "" {
		l.warnActorNotManaged(spec, actor)
		return nil
	}

	command, err := l.resolveCommand(ctx, spec.Image)
	if err != nil {
		return fmt.Errorf("resolve image command: %w", err)
	}
	tmpl, err := l.opts.actorTemplate(actor, spec.Image, command)
	if err != nil {
		return err
	}
	if l.opts.AteomImage != "" {
		if err := ensureUnstructured(ctx, l.dyn, workerPoolGVR, l.opts.workerPool()); err != nil {
			return fmt.Errorf("ensure worker pool: %w", err)
		}
	}
	if err := ensureUnstructured(ctx, l.dyn, actorTemplateGVR, tmpl); err != nil {
		return fmt.Errorf("ensure actor template: %w", err)
	}
	if l.actors != nil {
		if err := l.actors.EnsureAtespace(ctx, atespace); err != nil {
			return fmt.Errorf("ensure atespace %q: %w", atespace, err)
		}
		if err := l.actors.EnsureActor(ctx, atespace, actor, l.opts.TemplateNamespace, actor); err != nil {
			return fmt.Errorf("ensure actor %q: %w", actor, err)
		}
	}

	l.ensured.Store(key, struct{}{})
	return nil
}

// resolveCommand reads the run image's entrypoint (Entrypoint ++ Cmd) so the durable
// ActorTemplate can carry an explicit process command; ate does not derive it from
// the image the way the isolated substrates do. The cluster build wires the resolver;
// a Launcher without one (a unit test, or a build that could not reach a registry) is
// a programmer error rather than a silently empty command.
func (l *Launcher) resolveCommand(ctx context.Context, image string) ([]string, error) {
	if l.entry == nil {
		return nil, fmt.Errorf("no image entrypoint resolver configured")
	}
	return l.entry.resolve(ctx, image)
}

// actorTemplate composes the ate ActorTemplate that stands the app up as a durable
// actor: the app image (which MUST be digest-pinned — changing it invalidates the
// golden snapshot) running the AEI runtime serve loop on :80 (AEI_SERVE_ADDR),
// gated on a /readyz probe. pauseImage and snapshotsConfig.location are required by
// the CRD and come from admin class params. The image is the AgentApp's, used once
// to define the actor (not per run, rule R1) — which is why a durable class no
// longer ignores it. command is the image's own entrypoint (Entrypoint ++ Cmd), which
// the caller resolves from the registry: ate does not derive the process args from
// the image the way the isolated substrates do, so it must be set explicitly.
func (o Options) actorTemplate(name, image string, command []string) (*unstructured.Unstructured, error) {
	if !strings.Contains(image, "@") {
		return nil, fmt.Errorf("app image %q must be digest-pinned (contain '@') for a durable ActorTemplate: a tag invalidates the golden snapshot", image)
	}
	container := map[string]any{
		"name":  "runtime",
		"image": image,
		"env":   o.actorEnv(),
		"readyz": map[string]any{
			"httpGet": map[string]any{"path": readyzPath, "port": servePort},
		},
	}
	if len(command) > 0 {
		container["command"] = toAnySlice(command)
	}
	spec := map[string]any{
		"pauseImage":   o.PauseImage,
		"sandboxClass": o.SandboxClass,
		"containers":   []any{container},
		"snapshotsConfig": map[string]any{
			"location": snapshotLocationFor(o.SnapshotLocation, name),
			// Full on both triggers. AEI's runtime is stateless between runs (per-run
			// state rides the dispatch and results report via callbacks), so it declares
			// no durableDir volume — a "Data" onCommit would fail with "no durable-dir
			// volumes found for DATA snapshot". Full captures RAM + the fs delta.
			"onPause":  "Full",
			"onCommit": "Full",
		},
		"workerSelector": map[string]any{
			"matchLabels": map[string]any{workerPoolLabel: o.WorkerPoolName},
		},
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ate.dev/v1alpha1",
		"kind":       "ActorTemplate",
		"metadata": map[string]any{
			"name":      name,
			"namespace": o.TemplateNamespace,
			"labels":    map[string]any{managedByKey: managedByVal},
		},
		"spec": spec,
	}}, nil
}

// snapshotLocationFor derives a per-actor golden-snapshot path from the class's
// snapshotLocation PREFIX, so actors that share one durable class never collide on a
// single location: "gs://ate-snapshots/" + "gofer" -> "gs://ate-snapshots/gofer/".
func snapshotLocationFor(prefix, actor string) string {
	return strings.TrimRight(prefix, "/") + "/" + actor + "/"
}

// toAnySlice converts a string slice to the []any an unstructured object requires.
func toAnySlice(ss []string) []any {
	a := make([]any, len(ss))
	for i, s := range ss {
		a[i] = s
	}
	return a
}

// actorEnv builds the ActorTemplate container's environment: always AEI_SERVE_ADDR
// (its presence selects the runtime's serve loop over one-shot), plus an optional
// AEI_MAX_CONCURRENT_RUNS concurrency cap the serve loop enforces (admin-set). These
// are standing-actor config keys, not per-run values (rule R1): they are frozen into
// the golden snapshot, not injected per dispatch.
func (o Options) actorEnv() []any {
	env := []any{
		map[string]any{"name": "AEI_SERVE_ADDR", "value": serveAddr},
	}
	if o.MaxConcurrentRuns != "" {
		env = append(env, map[string]any{"name": "AEI_MAX_CONCURRENT_RUNS", "value": o.MaxConcurrentRuns})
	}
	return env
}

// resourceLimits builds a resources.limits map from optional cpu/memory quantities,
// or nil when both are empty. It feeds the WorkerPool pod template (spec.template.
// resources) — the defense-in-depth ceiling beneath the serve-loop concurrency cap —
// so values are opaque Kubernetes quantity strings ("500m", "512Mi").
func resourceLimits(cpu, memory string) map[string]any {
	limits := map[string]any{}
	if cpu != "" {
		limits["cpu"] = cpu
	}
	if memory != "" {
		limits["memory"] = memory
	}
	if len(limits) == 0 {
		return nil
	}
	return limits
}

// workerPool composes a WorkerPool that runs the substrate ateom (gVisor) image. Its
// metadata carries workerPoolLabel: ate-api copies a WorkerPool's own labels onto each
// of its Workers as the eligibility labels the ActorTemplate's workerSelector matches
// (it does NOT use the worker pod's labels). AEI creates the pool only when the admin
// supplies an ateomImage; otherwise the admin owns the pool and the template selects it.
func (o Options) workerPool() *unstructured.Unstructured {
	spec := map[string]any{
		"replicas":     o.WorkerReplicas,
		"ateomImage":   o.AteomImage,
		"sandboxClass": o.SandboxClass,
	}
	// Optional worker-pod resource ceiling: the defense-in-depth cap beneath the
	// serve-loop concurrency limit. ate exposes pod resources on the WorkerPool's pod
	// template (spec.template.resources) — the ActorTemplate container has no resources
	// field — so a durable actor's footprint is bounded on the worker that hosts it.
	// Applies only when AEI creates the pool (an ateomImage is set); an admin-owned pool
	// carries its own resources.
	if limits := resourceLimits(o.ActorCPULimit, o.ActorMemoryLimit); limits != nil {
		spec["template"] = map[string]any{"resources": map[string]any{"limits": limits}}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ate.dev/v1alpha1",
		"kind":       "WorkerPool",
		"metadata": map[string]any{
			"name":      o.WorkerPoolName,
			"namespace": o.TemplateNamespace,
			"labels":    map[string]any{workerPoolLabel: o.WorkerPoolName, managedByKey: managedByVal},
		},
		"spec": spec,
	}}
}

// ensureUnstructured idempotently creates obj: it leaves an existing object as-is
// (an ate ActorTemplate spec is immutable once created) and treats a create race as
// success.
func ensureUnstructured(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	ri := dyn.Resource(gvr).Namespace(obj.GetNamespace())
	if _, err := ri.Get(ctx, obj.GetName(), metav1.GetOptions{}); err == nil {
		return nil // already exists; immutable, leave as-is
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if _, err := ri.Create(ctx, obj, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// warn logs at Warn with the provider tag, falling back to the default logger.
func (l *Launcher) warn(msg string, args ...any) {
	log := l.log
	if log == nil {
		log = slog.Default()
	}
	log.Warn(msg, append([]any{"provider", providerName}, args...)...)
}

// warnUnprovisionableImage explains a dropped image when auto-provisioning is off:
// on a durable substrate the image defines the standing actor, so if AEI is not
// provisioning it, the operator must have already built the actor from that image
// (the image cannot ride the dispatch, rule R1).
func (l *Launcher) warnUnprovisionableImage(spec aei.RunSpec) {
	if spec.Image == "" {
		return
	}
	l.warn("agent-substrate: not auto-provisioning the actor, so the app image is honored only if the actor was already built from it (the actor is one standing runtime; the image cannot ride the dispatch)",
		"runID", spec.ID, "image", spec.Image, "actor", l.opts.actorIdentity())
}

// warnActorNotManaged explains why AEI is not provisioning this run's durable actor
// — the AgentApp has no image, or the class has no snapshotLocation — so the actor
// must already exist (pre-provisioned out of band, or via a class that sets
// snapshotLocation so AEI can create it). AEI provisions the whole chain or nothing,
// so it never creates an actor against a template it did not create.
func (l *Launcher) warnActorNotManaged(spec aei.RunSpec, actor string) {
	reason := "the class has no snapshotLocation"
	if spec.Image == "" {
		reason = "the AgentApp has no image"
	}
	l.warn("agent-substrate: not provisioning the durable actor ("+reason+"); the actor must be pre-provisioned out of band, or set snapshotLocation on the class so AEI can create it",
		"actor", actor, "atespace", l.opts.Atespace, "image", spec.Image)
}
