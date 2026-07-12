package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentProviderClassSpec is the admin's binding of a class name to a provider and
// its policy — the indirection that makes an AgentRun portable across clusters
// (docs/design.md). The app references the class by name; the admin owns what it
// maps to.
type AgentProviderClassSpec struct {
	// Provider is the registered driver that backs this class (e.g. "k8s-job",
	// "agent-sandbox", "kagent").
	// +kubebuilder:validation:MinLength=1
	Provider string `json:"provider"`

	// Parameters are driver-specific knobs (endpoint, image, namespace, control
	// URL, …) passed to the provider at build time.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// DefaultIsolationClass is applied when an AgentRun omits an isolationClass.
	// +optional
	DefaultIsolationClass string `json:"defaultIsolationClass,omitempty"`

	// IdentityIssuer names where per-run credentials are minted (e.g. an HMAC key
	// ref or a SPIFFE trust domain).
	// +optional
	IdentityIssuer string `json:"identityIssuer,omitempty"`

	// QueueName defers admission/quota to Kueue (docs/design.md, composition).
	// +optional
	QueueName string `json:"queueName,omitempty"`

	// Capabilities are the semantics this class advertises (ASYNC, PULL_CREDENTIAL,
	// DURABLE, …), verified by conformance and matched against AgentRun requirements.
	// +optional
	Capabilities []string `json:"capabilities,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=apc
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`

// AgentProviderClass binds a class name to a provider and its policy
// (cluster-scoped, mirroring StorageClass/RuntimeClass).
type AgentProviderClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AgentProviderClassSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// AgentProviderClassList is a list of AgentProviderClass.
type AgentProviderClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentProviderClass `json:"items"`
}

// IsDefaultClassAnnotation marks the cluster-default provider class, used when an
// AgentRun omits providerClassName.
const IsDefaultClassAnnotation = "agents.x-k8s.io/is-default-class"

func init() {
	SchemeBuilder.Register(&AgentProviderClass{}, &AgentProviderClassList{})
}
