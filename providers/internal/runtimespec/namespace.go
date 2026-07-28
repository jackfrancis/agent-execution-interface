package runtimespec

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// appToken is the placeholder a namespace parameter uses to opt into per-app
// placement: a class whose namespace is e.g. "{app}-runtime" lands each app's runs
// in its own "<app>-runtime" namespace, keeping the untrusted runtime out of the
// app's control-plane namespace (where its secrets and web tier live).
const appToken = "{app}"

// Namespace resolves the AgentProviderClass namespace parameter for one run — the
// admin's placement policy. It returns the target namespace and whether AEI owns
// (provisions) it:
//
//   - a value containing the {app} token is expanded to the run's origin app, so
//     each app gets its own runtime namespace (managed=true); an app-less run (the
//     declarative AgentRun path) pools into a shared "aei-runtime" (managed=true);
//   - a non-empty literal is used as-is and is admin-owned — AEI does not create or
//     lock it down (managed=false), so an admin can point runs at a namespace they
//     pre-provisioned with their own policy;
//   - an empty parameter falls back to "default" (managed=false).
//
// managed=true is the signal a provider uses to call EnsureRuntimeNamespace before
// placing the workload.
func Namespace(param string, spec aei.RunSpec) (ns string, managed bool) {
	if strings.Contains(param, appToken) {
		app := sanitizeNS(spec.App)
		if app == "" {
			return "aei-runtime", true // app-less runs pool into a shared runtime namespace
		}
		return sanitizeNS(strings.ReplaceAll(param, appToken, app)), true
	}
	if param != "" {
		return param, false // admin-owned literal namespace; AEI does not provision it
	}
	return "default", false
}

// sanitizeNS makes s a DNS-1123-label-safe namespace name: lowercased, invalid
// characters collapsed to '-', trimmed, and truncated to 63 characters.
func sanitizeNS(s string) string {
	s = nonDNS.ReplaceAllString(strings.ToLower(s), "-")
	s = strings.Trim(s, "-")
	if len(s) > 63 {
		s = strings.Trim(s[:63], "-")
	}
	return s
}

// denyIngressPolicy is the default-deny-ingress NetworkPolicy name EnsureRuntime-
// Namespace installs in an AEI-managed runtime namespace.
const denyIngressPolicy = "aei-default-deny-ingress"

// EnsureRuntimeNamespace makes ns a usable, isolated runtime namespace: it creates
// the namespace if missing and installs a default-deny-INGRESS NetworkPolicy so a
// runtime pod cannot be connected to by other tenants' workloads. Egress is left
// open on purpose — the runtime must reach its app's callback plane and external
// model/tool APIs — so tightening egress (blocking internal CIDRs and the node
// metadata server, as a SandboxTemplate would) is a deliberate follow-up. It is
// idempotent: an existing namespace or policy is left as-is. Callers invoke it only
// for AEI-managed namespaces (Namespace returned managed=true); an admin-owned
// literal namespace is never created or modified here.
func EnsureRuntimeNamespace(ctx context.Context, kube kubernetes.Interface, ns string) error {
	if ns == "" || ns == "default" {
		return nil
	}
	managedBy := map[string]string{"app.kubernetes.io/managed-by": "aei"}

	if _, err := kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: managedBy}}
		if _, err := kube.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("ensure runtime namespace %q: %w", ns, err)
		}
	} else if err != nil {
		return fmt.Errorf("ensure runtime namespace %q: %w", ns, err)
	}

	if _, err := kube.NetworkingV1().NetworkPolicies(ns).Get(ctx, denyIngressPolicy, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		np := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: denyIngressPolicy, Namespace: ns, Labels: managedBy},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{},                                    // all pods in the namespace
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, // no Ingress rules → deny all ingress
			},
		}
		if _, err := kube.NetworkingV1().NetworkPolicies(ns).Create(ctx, np, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("ensure default-deny-ingress in %q: %w", ns, err)
		}
	} else if err != nil {
		return fmt.Errorf("ensure default-deny-ingress in %q: %w", ns, err)
	}
	return nil
}
