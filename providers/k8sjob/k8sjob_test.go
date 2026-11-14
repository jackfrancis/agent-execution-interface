package k8sjob

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

func testLauncher() *Launcher {
	l := New(fake.NewSimpleClientset(), Options{
		Namespace:      "aei",
		Image:          "img:1",
		ServiceAccount: "aei-runtime",
		ControlURL:     "http://cp/aei/v1alpha1",
	})
	l.pollInterval = time.Millisecond
	l.now = func() time.Time { return time.UnixMilli(1789000000000) }
	return l
}

func envMap(c corev1.Container) map[string]string {
	m := map[string]string{}
	for _, e := range c.Env {
		m[e.Name] = e.Value
	}
	return m
}

// TestDispatchEncodesInjectionContract is the cross-provider regression check: the
// Job's runtime container carries the identical AEI_* injection contract every
// other provider emits, plus the one-shot Job shape (no restart, deadline).
func TestDispatchEncodesInjectionContract(t *testing.T) {
	l := testLauncher()
	spec := aei.RunSpec{
		ID:         "run-abc",
		TaskRef:    "worklist://item/pr-4127",
		Parameters: map[string]string{"kind": "converse"},
		Deadline:   5 * time.Minute,
		Identity:   aei.IdentityRequest{Audience: "team-a-agent"},
	}

	h, err := l.Dispatch(context.Background(), spec, aei.Credential{Value: "tok-123", Kind: aei.Token})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if h.Provider != "k8s-job" || h.Ref != "aei-run-abc" {
		t.Fatalf("handle = %+v, want {k8s-job aei-run-abc}", h)
	}

	job, err := l.client.BatchV1().Jobs("aei").Get(context.Background(), h.Ref, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", pod.RestartPolicy)
	}
	if pod.ServiceAccountName != "aei-runtime" {
		t.Errorf("serviceAccount = %q", pod.ServiceAccountName)
	}
	if got := pod.Containers[0].Image; got != "img:1" {
		t.Errorf("image = %q, want img:1", got)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 300 {
		t.Errorf("activeDeadlineSeconds = %v, want 300", job.Spec.ActiveDeadlineSeconds)
	}

	env := envMap(pod.Containers[0])
	want := map[string]string{
		aei.EnvRunID:        "run-abc",
		aei.EnvTaskRef:      "worklist://item/pr-4127",
		aei.EnvParams:       `{"kind":"converse"}`,
		aei.EnvControlURL:   "http://cp/aei/v1alpha1",
		aei.EnvCredential:   "tok-123",
		aei.EnvDispatchedAt: "1789000000000",
		aei.EnvDeadline:     "1789000300000",
		aei.EnvAudience:     "team-a-agent",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("env[%s] = %q, want %q", k, env[k], v)
		}
	}
	if _, ok := env[aei.EnvTicket]; ok {
		t.Error("push credential must not set AEI_RUN_TICKET")
	}
}

// TestDispatchAppImageOverridesClassDefault verifies the app-supplied RunSpec.Image
// wins over the provider-class default, so a generic (imageless) class works and
// the app brings its own runtime image.
func TestDispatchAppImageOverridesClassDefault(t *testing.T) {
	l := testLauncher() // Options.Image = "img:1" (the class default / fallback)
	h, err := l.Dispatch(context.Background(),
		aei.RunSpec{ID: "run-img", Image: "app-runtime:v9"}, aei.Credential{Value: "tok"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	job, _ := l.client.BatchV1().Jobs("aei").Get(context.Background(), h.Ref, metav1.GetOptions{})
	if got := job.Spec.Template.Spec.Containers[0].Image; got != "app-runtime:v9" {
		t.Errorf("image = %q, want app-runtime:v9 (RunSpec.Image overrides class default)", got)
	}
}

// TestDispatchTicketUsesTicketEnv verifies a pull credential rides AEI_RUN_TICKET,
// not AEI_RUN_CREDENTIAL.
func TestDispatchTicketUsesTicketEnv(t *testing.T) {
	l := testLauncher()
	h, err := l.Dispatch(context.Background(),
		aei.RunSpec{ID: "run-pull"}, aei.Credential{Value: "tkt", Kind: aei.Ticket})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	job, _ := l.client.BatchV1().Jobs("aei").Get(context.Background(), h.Ref, metav1.GetOptions{})
	env := envMap(job.Spec.Template.Spec.Containers[0])
	if env[aei.EnvTicket] != "tkt" {
		t.Errorf("AEI_RUN_TICKET = %q, want tkt", env[aei.EnvTicket])
	}
	if _, ok := env[aei.EnvCredential]; ok {
		t.Error("pull credential must not set AEI_RUN_CREDENTIAL")
	}
}

// TestAwaitReturnsOutcomeOnTerminalCondition drives Await to both terminal states
// by seeding the Job's status conditions.
func TestAwaitReturnsOutcomeOnTerminalCondition(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cond    batchv1.JobConditionType
		wantErr bool
	}{
		{"complete", batchv1.JobComplete, false},
		{"failed", batchv1.JobFailed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := testLauncher()
			h, err := l.Dispatch(context.Background(), aei.RunSpec{ID: "run-" + tc.name}, aei.Credential{Value: "tok"})
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			// Seed the terminal condition the runtime/controller would set.
			job, _ := l.client.BatchV1().Jobs("aei").Get(context.Background(), h.Ref, metav1.GetOptions{})
			job.Status.Conditions = []batchv1.JobCondition{{Type: tc.cond, Status: corev1.ConditionTrue, Reason: "test"}}
			if _, err := l.client.BatchV1().Jobs("aei").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("seed status: %v", err)
			}

			res, err := l.Await(context.Background(), h)
			if err != nil {
				t.Fatalf("Await returned an operational error: %v", err)
			}
			if tc.wantErr && res.Err == nil {
				t.Error("want Result.Err for a failed job")
			}
			if !tc.wantErr && res.Err != nil {
				t.Errorf("want success, got Result.Err = %v", res.Err)
			}
		})
	}
}

// TestAwaitDeadlineBackstop verifies that with no terminal condition, Await returns
// once the context deadline (the universal backstop) fires.
func TestAwaitDeadlineBackstop(t *testing.T) {
	l := testLauncher()
	h, err := l.Dispatch(context.Background(), aei.RunSpec{ID: "run-timeout"}, aei.Credential{Value: "tok"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res, err := l.Await(ctx, h)
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if res.Err == nil {
		t.Error("want Result.Err (timeout) when the job never completes")
	}
}

// TestRegisteredUnderName verifies the provider self-registered so it is
// selectable by name — the swap mechanism.
func TestRegisteredUnderName(t *testing.T) {
	found := false
	for _, n := range aei.Names() {
		if n == "k8s-job" {
			found = true
		}
	}
	if !found {
		t.Fatalf("k8s-job not registered; got %v", aei.Names())
	}
}

// TestTolerationsFromClassParameter proves the class's "tolerations" parameter
// reaches the Job's pod, and that a malformed value fails the class.
func TestTolerationsFromClassParameter(t *testing.T) {
	opts, err := optionsFromConfig(aei.Config{Params: map[string]string{"tolerations": "aei.dev/agent-runs:NoSchedule"}})
	if err != nil {
		t.Fatalf("optionsFromConfig: %v", err)
	}
	l := testLauncher()
	l.opts.Tolerations = opts.Tolerations
	job := l.job(aei.RunSpec{ID: "r"}, aei.Credential{Value: "tok"}, l.now())

	tols := job.Spec.Template.Spec.Tolerations
	if len(tols) != 1 || tols[0].Key != "aei.dev/agent-runs" || string(tols[0].Operator) != "Exists" || string(tols[0].Effect) != "NoSchedule" {
		t.Errorf("pod tolerations = %+v", tols)
	}

	if _, err := optionsFromConfig(aei.Config{Params: map[string]string{"tolerations": "=v:NoSchedule"}}); err == nil {
		t.Error("a malformed tolerations parameter must be an error")
	}
}
