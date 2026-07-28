package v1alpha1

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func classWith(name, defaultIso string, caps ...string) *AgentProviderClass {
	return &AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       AgentProviderClassSpec{DefaultIsolationClass: defaultIso, Capabilities: caps},
	}
}

// TestResolveIsolation: the run wins when it names a class, otherwise the admin's
// class default applies — the indirection that lets an admin raise isolation for
// every run without any app change.
func TestResolveIsolation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		class    *AgentProviderClass
		runClass string
		want     string
	}{
		{"class default applies", classWith("isolated", "gvisor"), "", "gvisor"},
		{"run overrides", classWith("isolated", "gvisor"), "kata", "kata"},
		{"neither", classWith("plain", ""), "", ""},
		{"run only", classWith("plain", ""), "gvisor", "gvisor"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.class.ResolveIsolation(tc.runClass); got != tc.want {
				t.Errorf("ResolveIsolation(%q) = %q, want %q", tc.runClass, got, tc.want)
			}
		})
	}
}

func TestCheckIsolation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		class       *AgentProviderClass
		isolation   string
		requirement string
		wantErr     string // substring; empty means the check must pass
	}{
		{
			name:  "no isolation requested",
			class: classWith("plain", ""),
		},
		{
			name:      "class enumerates nothing, defer to the provider",
			class:     classWith("isolated", "gvisor", "ASYNC"),
			isolation: "gvisor",
		},
		{
			name:      "advertised",
			class:     classWith("isolated", "gvisor", "ASYNC", "ISOLATION:gvisor"),
			isolation: "gvisor",
		},
		{
			name:      "not advertised",
			class:     classWith("isolated", "gvisor", "ISOLATION:kata"),
			isolation: "gvisor",
			wantErr:   "does not advertise isolation class",
		},
		{
			name:        "required but none resolved",
			class:       classWith("plain", ""),
			requirement: "required",
			wantErr:     "requires isolation",
		},
		{
			name:        "required and resolved",
			class:       classWith("isolated", "gvisor", "ISOLATION:gvisor"),
			isolation:   "gvisor",
			requirement: "required",
		},
		{
			name:        "preferred tolerates none",
			class:       classWith("plain", ""),
			requirement: "preferred",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.class.CheckIsolation(tc.isolation, tc.requirement)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("no error, want one containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}
