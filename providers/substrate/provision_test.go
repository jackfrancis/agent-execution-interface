package substrate

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// fakeEnsurer records the ate-api calls the provider would make (atespace/actor
// creation), standing in for the gRPC client.
type fakeEnsurer struct {
	atespaces []string
	actors    []string
}

func (f *fakeEnsurer) EnsureAtespace(_ context.Context, name string) error {
	f.atespaces = append(f.atespaces, name)
	return nil
}

func (f *fakeEnsurer) EnsureActor(_ context.Context, atespace, name, templateNS, templateName string) error {
	f.actors = append(f.actors, fmt.Sprintf("%s/%s<-%s/%s", atespace, name, templateNS, templateName))
	return nil
}

func fakeDyn() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			actorTemplateGVR: "ActorTemplateList",
			workerPoolGVR:    "WorkerPoolList",
		})
}

// TestActorTemplateComposition: the composed ActorTemplate carries the digest image
// running the serve loop on :80 with a /readyz probe, plus the CRD-required
// pauseImage + snapshotsConfig.location, and it selects its WorkerPool by label.
func TestActorTemplateComposition(t *testing.T) {
	opts := optionsFromConfig(aei.Config{Params: map[string]string{"snapshotLocation": "gs://bucket/aei/"}})

	u, err := opts.actorTemplate("gofer", "reg/gofer@sha256:abc", []string{"/runtime"})
	if err != nil {
		t.Fatalf("actorTemplate: %v", err)
	}
	if u.GetAPIVersion() != "ate.dev/v1alpha1" || u.GetKind() != "ActorTemplate" {
		t.Fatalf("gvk = %s/%s", u.GetAPIVersion(), u.GetKind())
	}
	if u.GetName() != "gofer" || u.GetNamespace() != defaultTemplateNamespace {
		t.Fatalf("meta = %s/%s", u.GetNamespace(), u.GetName())
	}
	if loc, _, _ := unstructured.NestedString(u.Object, "spec", "snapshotsConfig", "location"); loc != "gs://bucket/aei/gofer/" {
		t.Errorf("snapshot location = %q, want per-actor gs://bucket/aei/gofer/", loc)
	}
	if pause, _, _ := unstructured.NestedString(u.Object, "spec", "pauseImage"); pause != defaultPauseImage {
		t.Errorf("pauseImage = %q", pause)
	}
	if sc, _, _ := unstructured.NestedString(u.Object, "spec", "sandboxClass"); sc != "gvisor" {
		t.Errorf("sandboxClass = %q, want gvisor", sc)
	}
	// The selector key must be the platform's worker-pod label (ate.dev/worker-pool),
	// not AEI's own domain, or the scheduler finds no eligible worker ("no free workers").
	if lbl, _, _ := unstructured.NestedString(u.Object, "spec", "workerSelector", "matchLabels", "ate.dev/worker-pool"); lbl != defaultWorkerPool {
		t.Errorf("workerSelector[ate.dev/worker-pool] = %q, want %q", lbl, defaultWorkerPool)
	}
	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	c := containers[0].(map[string]any)
	if c["image"] != "reg/gofer@sha256:abc" {
		t.Errorf("image = %v", c["image"])
	}
	if cmd, _, _ := unstructured.NestedStringSlice(c, "command"); len(cmd) != 1 || cmd[0] != "/runtime" {
		t.Errorf("command = %v, want [/runtime] (the image entrypoint the resolver returns)", cmd)
	}
	env := c["env"].([]any)[0].(map[string]any)
	if env["name"] != "AEI_SERVE_ADDR" || env["value"] != ":80" {
		t.Errorf("env = %v", env)
	}
	port, _, _ := unstructured.NestedInt64(c, "readyz", "httpGet", "port")
	if port != 80 {
		t.Errorf("readyz port = %d, want 80", port)
	}
}

// TestActorTemplateRequiresDigest: a tag image is rejected (a tag invalidates the
// golden snapshot; the ate CRD requires a pinned image).
func TestActorTemplateRequiresDigest(t *testing.T) {
	opts := optionsFromConfig(aei.Config{Params: map[string]string{"snapshotLocation": "gs://b/"}})
	if _, err := opts.actorTemplate("gofer", "reg/gofer:dev", []string{"/runtime"}); err == nil {
		t.Fatal("want error for a non-digest image, got nil")
	}
}

// TestWorkerPoolMatchesSelector pins the scheduling invariant: ate-api gives a Worker
// the labels of its WorkerPool's OWN metadata (not the pod's), so the WorkerPool
// metadata must carry every label the ActorTemplate's workerSelector matches — else
// dispatch gets "no free workers available". Guards the regression where the selector
// and the pool's label drifted apart.
func TestWorkerPoolMatchesSelector(t *testing.T) {
	opts := optionsFromConfig(aei.Config{Params: map[string]string{
		"snapshotLocation": "gs://b/", "ateomImage": "ko://ateom@sha256:d",
	}})
	at, err := opts.actorTemplate("gofer", "reg/gofer@sha256:abc", []string{"/runtime"})
	if err != nil {
		t.Fatalf("actorTemplate: %v", err)
	}
	sel, _, _ := unstructured.NestedStringMap(at.Object, "spec", "workerSelector", "matchLabels")
	if len(sel) == 0 {
		t.Fatal("workerSelector has no matchLabels")
	}
	poolLabels, _, _ := unstructured.NestedStringMap(opts.workerPool().Object, "metadata", "labels")
	for k, v := range sel {
		if poolLabels[k] != v {
			t.Errorf("WorkerPool metadata %q=%q does not satisfy the ActorTemplate workerSelector %q=%q (ate matches the selector against the pool's own labels)", k, poolLabels[k], k, v)
		}
	}
}

// TestActorTemplateOverloadControls: the optional overload knobs appear only when set —
// AEI_MAX_CONCURRENT_RUNS on the ActorTemplate container (the serve-loop cap) and the
// resource ceiling on the WorkerPool pod template (ate has no ActorTemplate container
// resources field) — and are absent by default.
func TestActorTemplateOverloadControls(t *testing.T) {
	// Default: no cap env beyond AEI_SERVE_ADDR, no container resources, no pool template.
	defOpts := optionsFromConfig(aei.Config{Params: map[string]string{"snapshotLocation": "gs://b/"}})
	def, err := defOpts.actorTemplate("gofer", "reg/gofer@sha256:abc", []string{"/runtime"})
	if err != nil {
		t.Fatalf("actorTemplate: %v", err)
	}
	dc := mustContainer(t, def)
	if env := dc["env"].([]any); len(env) != 1 {
		t.Errorf("default env = %d entries, want 1 (AEI_SERVE_ADDR only)", len(env))
	}
	if _, ok := dc["resources"]; ok {
		t.Error("default container must not set resources")
	}
	if _, found, _ := unstructured.NestedMap(defOpts.workerPool().Object, "spec", "template"); found {
		t.Error("default WorkerPool must not set a pod template")
	}

	// Configured: the cap rides the actor env; the limits ride the WorkerPool pod template.
	opts := optionsFromConfig(aei.Config{Params: map[string]string{
		"snapshotLocation":  "gs://b/",
		"maxConcurrentRuns": "8",
		"actorCPU":          "500m",
		"actorMemory":       "512Mi",
	}})
	at, err := opts.actorTemplate("gofer", "reg/gofer@sha256:abc", []string{"/runtime"})
	if err != nil {
		t.Fatalf("actorTemplate: %v", err)
	}
	c := mustContainer(t, at)
	if got := actorEnvValue(c, "AEI_MAX_CONCURRENT_RUNS"); got != "8" {
		t.Errorf("AEI_MAX_CONCURRENT_RUNS = %q, want 8", got)
	}
	if _, ok := c["resources"]; ok {
		t.Error("resources must not be on the ActorTemplate container (ate has no such field)")
	}
	wp := opts.workerPool().Object
	if cpu, _, _ := unstructured.NestedString(wp, "spec", "template", "resources", "limits", "cpu"); cpu != "500m" {
		t.Errorf("WorkerPool template cpu limit = %q, want 500m", cpu)
	}
	if mem, _, _ := unstructured.NestedString(wp, "spec", "template", "resources", "limits", "memory"); mem != "512Mi" {
		t.Errorf("WorkerPool template memory limit = %q, want 512Mi", mem)
	}
}

func mustContainer(t *testing.T, u *unstructured.Unstructured) map[string]any {
	t.Helper()
	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	return containers[0].(map[string]any)
}

func actorEnvValue(c map[string]any, name string) string {
	for _, e := range c["env"].([]any) {
		m := e.(map[string]any)
		if m["name"] == name {
			v, _ := m["value"].(string)
			return v
		}
	}
	return ""
}

// fakeResolver is a stand-in entrypoint resolver: it returns a fixed command without
// touching a registry, recording the images it was asked to resolve.
type fakeResolver struct {
	cmd    []string
	images []string
}

func (f *fakeResolver) resolve(_ context.Context, image string) ([]string, error) {
	f.images = append(f.images, image)
	return f.cmd, nil
}

// TestEnsureCreatesTemplatePoolAndActor: a first dispatch materializes the
// WorkerPool + ActorTemplate (via the dynamic client) and the atespace + actor (via
// the ensurer), and a second dispatch for the same actor is a no-op (once-cached).
func TestEnsureCreatesTemplatePoolAndActor(t *testing.T) {
	opts := optionsFromConfig(aei.Config{Params: map[string]string{
		"actor":            "{app}",
		"snapshotLocation": "gs://bucket/aei/",
		"ateomImage":       "ko://ateom@sha256:deadbeef",
	}})
	l := New(&http.Client{}, opts)
	dyn := fakeDyn()
	l.dyn = dyn
	ens := &fakeEnsurer{}
	l.actors = ens
	l.entry = &fakeResolver{cmd: []string{"/runtime"}}

	spec := aei.RunSpec{App: "gofer", ID: "run-1", Image: "reg/gofer@sha256:abc"}
	if err := l.ensure(context.Background(), spec); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	at, err := dyn.Resource(actorTemplateGVR).Namespace(defaultTemplateNamespace).Get(context.Background(), "gofer", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ActorTemplate not created: %v", err)
	}
	cs, _, _ := unstructured.NestedSlice(at.Object, "spec", "containers")
	if len(cs) != 1 {
		t.Fatalf("ActorTemplate containers = %d, want 1", len(cs))
	}
	if cmd, _, _ := unstructured.NestedStringSlice(cs[0].(map[string]any), "command"); len(cmd) != 1 || cmd[0] != "/runtime" {
		t.Errorf("ActorTemplate command = %v, want [/runtime] (resolved from the image)", cmd)
	}
	if _, err := dyn.Resource(workerPoolGVR).Namespace(defaultTemplateNamespace).Get(context.Background(), defaultWorkerPool, metav1.GetOptions{}); err != nil {
		t.Errorf("WorkerPool not created: %v", err)
	}
	if len(ens.atespaces) != 1 || ens.atespaces[0] != "aei" {
		t.Errorf("atespaces = %v, want [aei]", ens.atespaces)
	}
	want := "aei/gofer<-aei-actors/gofer"
	if len(ens.actors) != 1 || ens.actors[0] != want {
		t.Errorf("actors = %v, want [%s]", ens.actors, want)
	}

	// Second ensure for the same actor identity is a no-op.
	ens.atespaces, ens.actors = nil, nil
	if err := l.ensure(context.Background(), spec); err != nil {
		t.Fatalf("ensure (2nd): %v", err)
	}
	if len(ens.atespaces) != 0 || len(ens.actors) != 0 {
		t.Errorf("2nd ensure re-provisioned: atespaces=%v actors=%v", ens.atespaces, ens.actors)
	}
}

// TestEnsureSkippedWithoutSnapshotLocation: with no snapshotLocation AEI cannot
// create the ActorTemplate, so it must NOT create the atespace/actor either (which
// would reference a missing template) — it provisions the whole chain or nothing.
func TestEnsureSkippedWithoutSnapshotLocation(t *testing.T) {
	opts := optionsFromConfig(aei.Config{Params: map[string]string{"actor": "{app}"}}) // no snapshotLocation
	l := New(&http.Client{}, opts)
	dyn := fakeDyn()
	l.dyn = dyn
	ens := &fakeEnsurer{}
	l.actors = ens

	if err := l.ensure(context.Background(), aei.RunSpec{App: "gofer", Image: "reg/gofer@sha256:abc"}); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if len(ens.atespaces) != 0 || len(ens.actors) != 0 {
		t.Errorf("must not provision the actor without a template: atespaces=%v actors=%v", ens.atespaces, ens.actors)
	}
	if _, err := dyn.Resource(actorTemplateGVR).Namespace(defaultTemplateNamespace).Get(context.Background(), "gofer", metav1.GetOptions{}); err == nil {
		t.Error("must not create an ActorTemplate without snapshotLocation")
	}
}

// TestEnsureSkippedWithoutClient: with no dynamic client (outside a cluster) ensure
// is a no-op, so dispatch still proceeds against a pre-provisioned actor.
func TestEnsureSkippedWithoutClient(t *testing.T) {
	l := New(&http.Client{}, optionsFromConfig(aei.Config{}))
	if err := l.ensure(context.Background(), aei.RunSpec{App: "gofer", Image: "reg/gofer@sha256:abc"}); err != nil {
		t.Fatalf("ensure with nil client should be a no-op, got %v", err)
	}
}
