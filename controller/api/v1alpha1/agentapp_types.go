package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentAppSpec is the app developer's long-lived registration — the one object
// that replaces a per-run AgentRun at scale (docs/dispatch-plane.md). It binds an
// app to a provider class and bounds the identity its runs may mint; the runs
// themselves are dispatched through the data-plane API, not as Kubernetes
// objects, so a single AgentApp fronts thousands of runs.
type AgentAppSpec struct {
	// ProviderClassName selects the AgentProviderClass this app's runs dispatch
	// to; empty selects the cluster default (the class annotated
	// IsDefaultClassAnnotation). The app never names a substrate.
	// +optional
	ProviderClassName string `json:"providerClassName,omitempty"`

	// Dispatchers are the names of ServiceAccounts in this namespace permitted to
	// dispatch as this app. The data-plane endpoint authenticates the caller's
	// workload token (TokenReview) and rejects any SA not listed — the token-level
	// analog of the AgentRun create RBAC the declarative path relied on.
	// +optional
	Dispatchers []string `json:"dispatchers,omitempty"`

	// Identity bounds the per-run credentials minted for this app's runs.
	// +optional
	Identity AgentAppIdentity `json:"identity,omitempty"`
}

// AgentAppIdentity is the ceiling on what a run of this app may be granted — least
// privilege, enforced at dispatch (the minted credential is
// requested ∩ MaxScopes, bound to Audience).
type AgentAppIdentity struct {
	// Audience the minted run credentials are bound to. The app fixes it; a
	// dispatch request cannot widen it.
	// +optional
	Audience string `json:"audience,omitempty"`

	// MaxScopes is the maximum set of scopes any run may request. A dispatch
	// asking for a scope outside this set is rejected.
	// +optional
	MaxScopes []string `json:"maxScopes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=aa
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.providerClassName`

// AgentApp is the app's dispatch registration (namespaced): one long-lived object
// per app, so high-volume dispatch is a data-plane call rather than an object per
// run (docs/dispatch-plane.md).
type AgentApp struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AgentAppSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// AgentAppList is a list of AgentApp.
type AgentAppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentApp `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentApp{}, &AgentAppList{})
}
