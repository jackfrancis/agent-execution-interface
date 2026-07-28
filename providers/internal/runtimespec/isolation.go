package runtimespec

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// VerifyIsolation checks that the cluster can satisfy a run's isolation class
// before the provider creates anything, so an unsatisfiable request fails the
// dispatch with a clear message rather than leaving a Pending workload behind.
//
// A pod-shaped provider resolves an isolation class through a RuntimeClass (see
// PodSpec), so the check is: does that RuntimeClass exist? That is the check that
// matters — a provider claiming it "supports gvisor" says nothing about whether
// this cluster has a gvisor handler installed, which is exactly how an isolated
// class ends up running under the default runtime.
//
// It fails closed: an empty isolation class needs no isolation and passes, but a
// requested class with no client to verify against is an error, because a provider
// that cannot confirm isolation must not quietly proceed without it.
func VerifyIsolation(ctx context.Context, kube kubernetes.Interface, isolation string) error {
	if isolation == "" {
		return nil // no isolation requested; the default runtime is what was asked for
	}
	if kube == nil {
		return fmt.Errorf("isolation class %q requested but the provider has no Kubernetes client to verify it", isolation)
	}
	_, err := kube.NodeV1().RuntimeClasses().Get(ctx, isolation, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("isolation class %q: no RuntimeClass %q in this cluster (install the runtime handler, or clear the class's defaultIsolationClass)", isolation, isolation)
	case err != nil:
		return fmt.Errorf("isolation class %q: verify RuntimeClass: %w", isolation, err)
	}
	return nil
}
