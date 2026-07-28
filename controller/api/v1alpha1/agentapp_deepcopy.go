// Deepcopy methods for the AgentApp types. Hand-written to match controller-gen's
// output (the repo's zz_generated.deepcopy.go), so `make generate` can later
// regenerate them identically. Kept in a separate file so regeneration never
// clobbers hand edits.

package v1alpha1

import runtime "k8s.io/apimachinery/pkg/runtime"

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AgentApp) DeepCopyInto(out *AgentApp) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

// DeepCopy creates a new AgentApp copying the receiver.
func (in *AgentApp) DeepCopy() *AgentApp {
	if in == nil {
		return nil
	}
	out := new(AgentApp)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a new runtime.Object copying the receiver.
func (in *AgentApp) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AgentAppList) DeepCopyInto(out *AgentAppList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]AgentApp, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a new AgentAppList copying the receiver.
func (in *AgentAppList) DeepCopy() *AgentAppList {
	if in == nil {
		return nil
	}
	out := new(AgentAppList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject creates a new runtime.Object copying the receiver.
func (in *AgentAppList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AgentAppSpec) DeepCopyInto(out *AgentAppSpec) {
	*out = *in
	if in.Dispatchers != nil {
		in, out := &in.Dispatchers, &out.Dispatchers
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	in.Identity.DeepCopyInto(&out.Identity)
}

// DeepCopy creates a new AgentAppSpec copying the receiver.
func (in *AgentAppSpec) DeepCopy() *AgentAppSpec {
	if in == nil {
		return nil
	}
	out := new(AgentAppSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AgentAppIdentity) DeepCopyInto(out *AgentAppIdentity) {
	*out = *in
	if in.MaxScopes != nil {
		in, out := &in.MaxScopes, &out.MaxScopes
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy creates a new AgentAppIdentity copying the receiver.
func (in *AgentAppIdentity) DeepCopy() *AgentAppIdentity {
	if in == nil {
		return nil
	}
	out := new(AgentAppIdentity)
	in.DeepCopyInto(out)
	return out
}
