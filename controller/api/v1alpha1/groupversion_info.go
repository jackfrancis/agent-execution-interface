// Package v1alpha1 contains the AEI declarative API — the AgentRun and
// AgentProviderClass CRDs (group agents.x-k8s.io). They are a projection on top
// of the aei.Launcher interface and the control plane: a reconciler implements
// them by resolving the class to a provider and driving the run through the same
// control.Plane an embedded Go app would use (docs/design.md, the declarative
// surface).
//
// +kubebuilder:object:generate=true
// +groupName=agents.x-k8s.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the API group/version for the AEI CRDs.
var GroupVersion = schema.GroupVersion{Group: "agents.x-k8s.io", Version: "v1alpha1"}

// SchemeBuilder registers the AEI types with a runtime scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds the AEI types to a scheme.
var AddToScheme = SchemeBuilder.AddToScheme

// addKnownTypes registers every AEI kind under GroupVersion. It uses the
// apimachinery scheme builder directly so this API package depends only on
// k8s.io/apimachinery, not on controller-runtime.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&AgentRun{}, &AgentRunList{},
		&AgentProviderClass{}, &AgentProviderClassList{},
		&AgentApp{}, &AgentAppList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
