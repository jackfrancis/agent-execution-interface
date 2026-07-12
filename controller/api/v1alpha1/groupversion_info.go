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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the API group/version for the AEI CRDs.
var GroupVersion = schema.GroupVersion{Group: "agents.x-k8s.io", Version: "v1alpha1"}

// SchemeBuilder registers the AEI types with a runtime scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the AEI types to a scheme.
var AddToScheme = SchemeBuilder.AddToScheme
