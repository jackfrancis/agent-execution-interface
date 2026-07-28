package v1alpha1

import (
	"fmt"
	"strings"
)

// IsolationCapabilityPrefix marks an entry in AgentProviderClass.Capabilities that
// advertises a satisfiable isolation class, e.g. "ISOLATION:gvisor"
// (docs/design.md, the capability model).
const IsolationCapabilityPrefix = "ISOLATION:"

// ResolveIsolation returns the isolation class in effect for a run: the run's own
// isolationClass, or the class default when the run omits one. This is the single
// resolution site — both dispatch ingresses (the AgentRun reconciler and the
// data-plane handler) use it, so an admin's defaultIsolationClass applies no matter
// how a run is submitted.
func (c *AgentProviderClass) ResolveIsolation(runClass string) string {
	if runClass != "" {
		return runClass
	}
	if c == nil {
		return ""
	}
	return c.Spec.DefaultIsolationClass
}

// CheckIsolation admits or rejects a resolved isolation class against the class's
// advertised capabilities and the run's declared requirement. It exists so an
// unsatisfiable request fails before anything is created, rather than downgrading
// to the default runtime — running unisolated is fine when that is what was asked
// for; running unisolated while claiming isolation is not.
//
// Two rules, deliberately non-redundant with the provider's own check:
//
//   - a run whose requirements.isolation is "required" must resolve to some
//     isolation class;
//   - when the class enumerates ISOLATION:<class> capabilities, the resolved class
//     must be among them. A class that enumerates none makes no claim here, so the
//     decision defers to the provider, which is the only party that can tell
//     whether this cluster can actually honor the class (for a pod-shaped provider,
//     whether the matching RuntimeClass exists).
func (c *AgentProviderClass) CheckIsolation(isolation, requirement string) error {
	if requirement == "required" && isolation == "" {
		return fmt.Errorf("run requires isolation but neither the run nor provider class %q names an isolation class", c.className())
	}
	if isolation == "" {
		return nil
	}
	advertised := c.isolationCapabilities()
	if len(advertised) == 0 {
		return nil // no claim to check; the provider validates it at dispatch
	}
	for _, a := range advertised {
		if a == isolation {
			return nil
		}
	}
	return fmt.Errorf("provider class %q does not advertise isolation class %q (it advertises %s); add %s%s to spec.capabilities if the provider can satisfy it",
		c.className(), isolation, strings.Join(advertised, ", "), IsolationCapabilityPrefix, isolation)
}

// isolationCapabilities returns the isolation classes the class advertises.
func (c *AgentProviderClass) isolationCapabilities() []string {
	if c == nil {
		return nil
	}
	var out []string
	for _, cap := range c.Spec.Capabilities {
		if rest, ok := strings.CutPrefix(cap, IsolationCapabilityPrefix); ok && rest != "" {
			out = append(out, rest)
		}
	}
	return out
}

func (c *AgentProviderClass) className() string {
	if c == nil {
		return ""
	}
	return c.Name
}
