package runtimespec

import (
	"context"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestNamespaceResolution covers the admin placement policy the class parameter
// expresses: a per-app template, its app-less pool, admin-owned literals, and the
// empty fallback — plus which of those AEI itself provisions (managed).
func TestNamespaceResolution(t *testing.T) {
	for _, tc := range []struct {
		name        string
		param       string
		app         string
		wantNS      string
		wantManaged bool
	}{
		{"per-app template", "{app}-runtime", "gofer", "gofer-runtime", true},
		{"per-app pools app-less runs", "{app}-runtime", "", "aei-runtime", true},
		{"per-app sanitizes the app", "{app}-runtime", "Gofer_App", "gofer-app-runtime", true},
		{"literal is admin-owned", "team-a", "gofer", "team-a", false},
		{"empty falls back to default", "", "gofer", "default", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, managed := Namespace(tc.param, aei.RunSpec{App: tc.app})
			if ns != tc.wantNS || managed != tc.wantManaged {
				t.Fatalf("Namespace(%q, app=%q) = (%q, %v), want (%q, %v)",
					tc.param, tc.app, ns, managed, tc.wantNS, tc.wantManaged)
			}
		})
	}
}

// TestEnsureRuntimeNamespace verifies a managed namespace is created with a
// default-deny-ingress NetworkPolicy (egress left open), and that it is idempotent.
func TestEnsureRuntimeNamespace(t *testing.T) {
	kube := fake.NewSimpleClientset()
	ctx := context.Background()
	if err := EnsureRuntimeNamespace(ctx, kube, "gofer-runtime"); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if _, err := kube.CoreV1().Namespaces().Get(ctx, "gofer-runtime", metav1.GetOptions{}); err != nil {
		t.Fatalf("namespace not created: %v", err)
	}
	np, err := kube.NetworkingV1().NetworkPolicies("gofer-runtime").Get(ctx, denyIngressPolicy, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("networkpolicy not created: %v", err)
	}
	if len(np.Spec.Ingress) != 0 {
		t.Errorf("want no ingress rules (deny all), got %d", len(np.Spec.Ingress))
	}
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("want policyTypes=[Ingress] (egress open), got %v", np.Spec.PolicyTypes)
	}
	if err := EnsureRuntimeNamespace(ctx, kube, "gofer-runtime"); err != nil {
		t.Fatalf("second ensure is not idempotent: %v", err)
	}
}
