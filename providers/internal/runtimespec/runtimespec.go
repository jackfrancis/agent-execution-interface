// Package runtimespec builds the runtime container and PodSpec every Kubernetes
// provider embeds — a Job's template, a Sandbox's spec.podTemplate, and so on —
// so the runtime container and the AEI_* injection contract are byte-identical
// across providers. That identity is the cross-provider conformance invariant the
// aei-sanity suite checks: swapping providers must never change what the runtime
// sees. It is internal to the providers module so it never leaks into an app graph.
package runtimespec

import (
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// nonDNS matches runs of characters not allowed in a DNS-1123 label.
var nonDNS = regexp.MustCompile(`[^a-z0-9-]+`)

// Name derives a DNS-1123-label-safe object name from a prefix and a run id (which
// is unique and the completion-correlation key), lowercased, sanitized, and
// truncated to 63 characters. Providers use it so a run maps to a stable,
// greppable workload name instead of a random generateName suffix.
func Name(prefix, runID string) string {
	s := nonDNS.ReplaceAllString(strings.ToLower(runID), "-")
	name := prefix + "-" + s
	if len(name) > 63 {
		name = name[:63]
	}
	name = strings.Trim(name, "-")
	if name == "" {
		return prefix + "-run"
	}
	return name
}

// Options are the PodSpec-shaping settings a provider forwards: the runtime image,
// its ServiceAccount, and the AEI control/broker URLs the runtime calls back.
type Options struct {
	Image          string
	ServiceAccount string
	ControlURL     string
	BrokerURL      string
}

// EnvVars adapts the substrate-neutral aei.Injection map (docs/runtime-abi.md §1)
// to container env vars, sorted for a deterministic spec (stable diffs and tests).
func EnvVars(opts Options, spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) []corev1.EnvVar {
	inj := aei.Injection(spec, cred, opts.ControlURL, opts.BrokerURL, dispatchedAt)
	ev := make([]corev1.EnvVar, 0, len(inj))
	for k, v := range inj {
		ev = append(ev, corev1.EnvVar{Name: k, Value: v})
	}
	sort.Slice(ev, func(i, j int) bool { return ev[i].Name < ev[j].Name })
	return ev
}

// Container is the single runtime container every provider runs; only the
// surrounding workload (Job, Sandbox, …) differs.
func Container(opts Options, spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) corev1.Container {
	return corev1.Container{
		Name:  "runtime",
		Image: opts.Image,
		Env:   EnvVars(opts, spec, cred, dispatchedAt),
	}
}

// PodSpec is the runtime pod every provider runs: one runtime container, no
// restart (a run executes once), under the configured ServiceAccount.
func PodSpec(opts Options, spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) corev1.PodSpec {
	return corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: opts.ServiceAccount,
		Containers:         []corev1.Container{Container(opts, spec, cred, dispatchedAt)},
	}
}
