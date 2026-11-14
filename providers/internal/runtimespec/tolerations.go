package runtimespec

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ParseTolerations reads the provider-class "tolerations" parameter: the taints a
// run's pod tolerates, so an admin can reserve nodes for agent runs by tainting
// them (and keep everything else off) without each app knowing about it.
//
// The value is a comma-separated list in the same form `kubectl taint` uses for
// the taints themselves, so the parameter can be copied from the node pool:
//
//	key=value:Effect   tolerate that exact taint
//	key:Effect         tolerate the key with any value
//	key                tolerate the key with any value and any effect
//
// Effect is NoSchedule, PreferNoSchedule or NoExecute. Spaces around the parts are
// ignored. An empty value means no tolerations.
//
// A malformed entry is an error, not skipped, and "malformed" is what the API server
// would reject: the key must be a qualified name and an Equal value a label value,
// the checks pod validation makes. Catching it here fails the class when it is
// loaded. Left to pod creation, a bad toleration fails a k8s-job run with an API
// error, but under agent-sandbox the Sandbox is accepted, its pod is never created,
// and the run just waits out its deadline with no reason given.
func ParseTolerations(s string) ([]corev1.Toleration, error) {
	var out []corev1.Toleration
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		t := corev1.Toleration{Operator: corev1.TolerationOpExists}
		keyValue := entry
		if i := strings.LastIndex(entry, ":"); i >= 0 {
			keyValue = entry[:i]
			effect := strings.TrimSpace(entry[i+1:])
			t.Effect = corev1.TaintEffect(effect)
			switch t.Effect {
			case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
			default:
				return nil, fmt.Errorf("toleration %q: effect %q is not NoSchedule, PreferNoSchedule or NoExecute", entry, effect)
			}
		}
		if i := strings.Index(keyValue, "="); i >= 0 {
			t.Key, t.Value, t.Operator = strings.TrimSpace(keyValue[:i]), strings.TrimSpace(keyValue[i+1:]), corev1.TolerationOpEqual
		} else {
			t.Key = strings.TrimSpace(keyValue)
		}
		if t.Key == "" {
			return nil, fmt.Errorf("toleration %q: no taint key", entry)
		}
		if errs := validation.IsQualifiedName(t.Key); len(errs) > 0 {
			return nil, fmt.Errorf("toleration %q: key %q: %s", entry, t.Key, strings.Join(errs, "; "))
		}
		if t.Operator == corev1.TolerationOpEqual {
			if errs := validation.IsValidLabelValue(t.Value); len(errs) > 0 {
				return nil, fmt.Errorf("toleration %q: value %q: %s", entry, t.Value, strings.Join(errs, "; "))
			}
		}
		out = append(out, t)
	}
	return out, nil
}
