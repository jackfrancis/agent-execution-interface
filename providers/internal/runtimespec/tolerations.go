package runtimespec

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
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
// Effect is NoSchedule, PreferNoSchedule or NoExecute. An empty value means no
// tolerations. A malformed entry is an error, not skipped: a toleration silently
// dropped would leave every run of the class Pending on a tainted pool.
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
			t.Effect = corev1.TaintEffect(entry[i+1:])
			switch t.Effect {
			case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
			default:
				return nil, fmt.Errorf("toleration %q: effect %q is not NoSchedule, PreferNoSchedule or NoExecute", entry, entry[i+1:])
			}
		}
		if i := strings.Index(keyValue, "="); i >= 0 {
			t.Key, t.Value, t.Operator = keyValue[:i], keyValue[i+1:], corev1.TolerationOpEqual
		} else {
			t.Key = keyValue
		}
		if t.Key == "" {
			return nil, fmt.Errorf("toleration %q: no taint key", entry)
		}
		out = append(out, t)
	}
	return out, nil
}
