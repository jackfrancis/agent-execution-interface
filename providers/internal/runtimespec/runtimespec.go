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

// WorkloadName is the object (and pod) name for a run: "<app>-<runid>" when the run
// carries its origin app, else "aei-<runid>". It is DNS-1123-safe (see Name), so a
// run maps to a stable, greppable, app-attributable workload name.
func WorkloadName(spec aei.RunSpec) string {
	prefix := nonDNS.ReplaceAllString(strings.ToLower(spec.App), "-")
	prefix = strings.Trim(prefix, "-")
	if prefix == "" {
		prefix = "aei"
	}
	return Name(prefix, spec.ID)
}

// Labels is the standard label set every provider stamps on a run's workload and
// its pod: managed-by=aei (so AEI can select its runs) plus, when the run carries
// its origin app, part-of=<app> so runs are attributable to it — the way to tell
// apps apart when several share one runtime namespace.
func Labels(spec aei.RunSpec) map[string]string {
	l := map[string]string{"app.kubernetes.io/managed-by": "aei"}
	if app := labelValue(spec.App); app != "" {
		l["app.kubernetes.io/part-of"] = app
	}
	return l
}

// labelValue makes s safe as a Kubernetes label value: at most 63 characters with
// no leading/trailing separator. An app name is already a DNS-1123 name, so this
// only guards the rare over-long case.
func labelValue(s string) string {
	if len(s) > 63 {
		s = s[:63]
	}
	return strings.Trim(s, "-_.")
}

// Options are the PodSpec-shaping settings a provider forwards: the runtime image,
// its ServiceAccount, and the AEI control/broker URLs the runtime calls back.
type Options struct {
	Image          string
	ServiceAccount string
	ControlURL     string
	BrokerURL      string
	// Tolerations are put on every run's pod (see ParseTolerations): the admin's
	// placement policy for the class, e.g. onto a node pool tainted for agent runs.
	Tolerations []corev1.Toleration
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
// surrounding workload (Job, Sandbox, …) differs. The image is the app-supplied
// RunSpec.Image (a generic class is not app-specific), falling back to the
// provider-class default when the app supplies none.
func Container(opts Options, spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) corev1.Container {
	image := spec.Image
	if image == "" {
		image = opts.Image
	}
	return corev1.Container{
		Name:  "runtime",
		Image: image,
		Env:   EnvVars(opts, spec, cred, dispatchedAt),
	}
}

// PodSpec is the runtime pod every provider runs: one runtime container, no
// restart (a run executes once), under the configured ServiceAccount.
//
// The run's IsolationClass becomes the pod's RuntimeClassName — the named-selector
// resolution docs/design.md specifies. Naming a RuntimeClass the cluster does not
// have is rejected at pod admission, so an isolation class that cannot be honored
// fails the run instead of silently downgrading it to the default runtime; use
// VerifyIsolation to catch that at dispatch, before any workload is created.
func PodSpec(opts Options, spec aei.RunSpec, cred aei.Credential, dispatchedAt time.Time) corev1.PodSpec {
	ps := corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: opts.ServiceAccount,
		Containers:         []corev1.Container{Container(opts, spec, cred, dispatchedAt)},
		Tolerations:        opts.Tolerations,
	}
	if iso := spec.Isolation; iso != "" {
		ps.RuntimeClassName = &iso
	}
	return ps
}
