package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentRunSpec is the app's intent: run this task, as this identity, on the
// provider bound to this class (docs/design.md). The payload is opaque —
// taskRef and parameters are handles AEI never parses.
type AgentRunSpec struct {
	// ProviderClassName references an AgentProviderClass by name; empty selects the
	// cluster default (the class annotated IsDefaultClassAnnotation).
	// +optional
	ProviderClassName string `json:"providerClassName,omitempty"`

	// TaskRef is an opaque handle to the work.
	// +optional
	TaskRef string `json:"taskRef,omitempty"`

	// Image is the runtime (agent) container image to run, supplied by the app so
	// the provider class stays a generic substrate binding; empty falls back to a
	// class default image where one is configured.
	// +optional
	Image string `json:"image,omitempty"`

	// Session, when set, binds this run to a durable conversation/session — an
	// advisory affinity hint a warm provider (agent-substrate) uses to route every
	// run of a session to the same standing actor, keeping the runtime's per-session
	// working set warm across turns. A stateless provider ignores it; correctness
	// never depends on it, only latency. Empty ⇒ a one-shot run.
	// +optional
	Session string `json:"session,omitempty"`

	// Parameters are per-run values that ride the dispatch (rule R1).
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// IsolationClass names the required isolation; empty defaults from the class.
	// +optional
	IsolationClass string `json:"isolationClass,omitempty"`

	// Identity is the per-run credential to mint (delegated authorization).
	// +optional
	Identity IdentitySpec `json:"identity,omitempty"`

	// Requirements are app-declared semantics admission enforces against the class.
	// +optional
	Requirements Requirements `json:"requirements,omitempty"`

	// TimeoutSeconds bounds the run's wall clock and is the completion backstop.
	// +optional
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
}

// IdentitySpec is the per-run credential request.
type IdentitySpec struct {
	// +optional
	Subject string `json:"subject,omitempty"`
	// +optional
	Scopes []string `json:"scopes,omitempty"`
	// +optional
	Audience string `json:"audience,omitempty"`
}

// Requirements are the semantics an app requires of the resolved provider.
type Requirements struct {
	// +kubebuilder:validation:Enum=perRun;any
	// +optional
	RuntimeLifecycle string `json:"runtimeLifecycle,omitempty"`
	// +kubebuilder:validation:Enum=required;preferred;none
	// +optional
	Isolation string `json:"isolation,omitempty"`
	// +kubebuilder:validation:Enum=none
	// +optional
	StatePersistence string `json:"statePersistence,omitempty"`
}

// HandleStatus records where a run was placed.
type HandleStatus struct {
	// +optional
	Provider string `json:"provider,omitempty"`
	// +optional
	Ref string `json:"ref,omitempty"`
}

// TimingStatus is the launcher-symmetric run-timing breakdown.
type TimingStatus struct {
	// +optional
	ProvisioningSeconds float64 `json:"provisioningSeconds,omitempty"`
	// +optional
	RuntimeSeconds float64 `json:"runtimeSeconds,omitempty"`
	// +optional
	ModelSeconds float64 `json:"modelSeconds,omitempty"`
	// +optional
	ModelCalls int `json:"modelCalls,omitempty"`
	// +optional
	ToolCalls int `json:"toolCalls,omitempty"`
}

// AgentRunPhase is the coarse lifecycle state.
type AgentRunPhase string

const (
	PhasePending   AgentRunPhase = "Pending"
	PhaseRunning   AgentRunPhase = "Running"
	PhaseSucceeded AgentRunPhase = "Succeeded"
	PhaseFailed    AgentRunPhase = "Failed"
)

// AgentRunStatus is the observed state of a run.
type AgentRunStatus struct {
	// +optional
	Phase AgentRunPhase `json:"phase,omitempty"`
	// +optional
	Handle HandleStatus `json:"handle,omitempty"`
	// ResultRef is an opaque pointer to where the result lives; AEI never parses it.
	// +optional
	ResultRef string `json:"resultRef,omitempty"`
	// +optional
	Timing TimingStatus `json:"timing,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ar
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.status.handle.provider`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AgentRun is the app's intent to run one unit of agent work (namespaced,
// mirroring PersistentVolumeClaim).
type AgentRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentRunSpec   `json:"spec,omitempty"`
	Status AgentRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AgentRunList is a list of AgentRun.
type AgentRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentRun `json:"items"`
}
