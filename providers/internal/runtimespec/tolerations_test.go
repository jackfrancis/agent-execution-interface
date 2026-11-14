package runtimespec

import (
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

var fixedTime = time.Unix(1789000000, 0)

func TestParseTolerations(t *testing.T) {
	got, err := ParseTolerations(" aei.dev/agent-runs=true:NoSchedule , spot:NoExecute,anything ,")
	if err != nil {
		t.Fatalf("ParseTolerations: %v", err)
	}
	want := []corev1.Toleration{
		{Key: "aei.dev/agent-runs", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
		{Key: "spot", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		{Key: "anything", Operator: corev1.TolerationOpExists},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

// Spaces around the parts are not part of the key, value or effect.
func TestParseTolerationsTrimsParts(t *testing.T) {
	got, err := ParseTolerations("aei.dev/agent-runs = true : NoSchedule")
	if err != nil {
		t.Fatalf("ParseTolerations: %v", err)
	}
	want := []corev1.Toleration{{Key: "aei.dev/agent-runs", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestParseTolerationsEmpty(t *testing.T) {
	for _, s := range []string{"", "  ", ","} {
		got, err := ParseTolerations(s)
		if err != nil || len(got) != 0 {
			t.Errorf("ParseTolerations(%q) = %v, %v; want none", s, got, err)
		}
	}
}

// A malformed entry must fail rather than be dropped: a missing toleration leaves
// every run Pending on a tainted pool, with nothing pointing at the class.
func TestParseTolerationsRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"key=value:Sometimes", "=value:NoSchedule", ":NoSchedule", "good:NoSchedule,bad:nope",
		// Each of these parsed before key and value were validated, and each
		// would then be rejected by the API server at pod creation.
		"key=a:b:NoSchedule",                             // value "a:b"
		"a=x:NoSchedule;b=y:NoSchedule",                  // ';' instead of ','
		"k k:NoSchedule",                                 // space inside the key
		"-bad=true:NoSchedule",                           // key must start alphanumeric
		"aei.dev/agent-runs=has space:NoSchedule",        // space inside the value
		"key=" + strings.Repeat("a", 64) + ":NoSchedule", // value over 63 characters
	} {
		if _, err := ParseTolerations(s); err == nil {
			t.Errorf("ParseTolerations(%q) succeeded, want an error", s)
		}
	}
}

func TestPodSpecCarriesTolerations(t *testing.T) {
	tol := []corev1.Toleration{{Key: "aei.dev/agent-runs", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
	ps := PodSpec(Options{Tolerations: tol}, aei.RunSpec{ID: "r"}, aei.Credential{}, fixedTime)
	if !reflect.DeepEqual(ps.Tolerations, tol) {
		t.Errorf("pod tolerations = %+v, want %+v", ps.Tolerations, tol)
	}
	if ps := PodSpec(Options{}, aei.RunSpec{ID: "r"}, aei.Credential{}, fixedTime); ps.Tolerations != nil {
		t.Errorf("no tolerations configured, but pod has %+v", ps.Tolerations)
	}
}
