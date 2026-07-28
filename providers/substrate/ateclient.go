package substrate

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	ateapipb "github.com/jackfrancis/agent-execution-interface/providers/substrate/internal/ateapipb"
)

// ateClient creates atespaces and actors through the ate-api-server gRPC control
// plane (they are not Kubernetes CRDs). It implements actorEnsurer. The connection
// is dialed lazily and reused; each RPC carries a freshly minted ServiceAccount
// bearer token bound to the ate-api audience, so a long-lived connection never
// carries a stale token.
type ateClient struct {
	opts Options
	kube kubernetes.Interface

	mu   sync.Mutex
	conn *grpc.ClientConn
	ctrl ateapipb.ControlClient // set on first use, or injected in tests
}

// newAteClient returns the ate-api client. It never dials here — the first Ensure*
// call opens the connection — so building the provider outside a reachable ate-api
// still succeeds and only a real dispatch pays the connection cost.
func newAteClient(opts Options, kube kubernetes.Interface) actorEnsurer {
	return &ateClient{opts: opts, kube: kube}
}

// control lazily dials the ate-api-server. ate-api serves TLS with pod certificates
// reached by in-cluster Service DNS (not a publicly trusted name), so verification
// is skipped; authentication is a per-RPC ServiceAccount bearer token.
func (a *ateClient) control() (ateapipb.ControlClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ctrl != nil {
		return a.ctrl, nil
	}
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) // #nosec G402 -- pod-cert server; authn is the SA bearer token
	conn, err := grpc.NewClient(a.opts.APIEndpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(&saTokenCreds{kube: a.kube, ns: a.opts.APITokenNS, sa: a.opts.APITokenSA, audience: a.opts.APIAudience}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial ate-api %s: %w", a.opts.APIEndpoint, err)
	}
	a.conn = conn
	a.ctrl = ateapipb.NewControlClient(conn)
	return a.ctrl, nil
}

// EnsureAtespace creates the atespace, tolerating an existing one (idempotent).
func (a *ateClient) EnsureAtespace(ctx context.Context, name string) error {
	ctrl, err := a.control()
	if err != nil {
		return err
	}
	_, err = ctrl.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}},
	})
	return ignoreAlreadyExists(err)
}

// EnsureActor creates the actor from its template, tolerating an existing one.
func (a *ateClient) EnsureActor(ctx context.Context, atespace, name, templateNamespace, templateName string) error {
	ctrl, err := a.control()
	if err != nil {
		return err
	}
	_, err = ctrl.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplateNamespace: templateNamespace,
			ActorTemplateName:      templateName,
		},
	})
	return ignoreAlreadyExists(err)
}

// ignoreAlreadyExists treats a gRPC AlreadyExists as success, so ensure is
// idempotent across re-dispatches.
func ignoreAlreadyExists(err error) error {
	if err == nil || status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

// saTokenCreds is a gRPC per-RPC credential that mints a short-lived ServiceAccount
// token (TokenRequest) bound to the ate-api audience for each call, mirroring how
// kubectl-ate authenticates to ate-api-server.
type saTokenCreds struct {
	kube     kubernetes.Interface
	ns, sa   string
	audience string
}

func (c *saTokenCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	exp := int64(3600)
	tr, err := c.kube.CoreV1().ServiceAccounts(c.ns).CreateToken(ctx, c.sa,
		&authv1.TokenRequest{Spec: authv1.TokenRequestSpec{Audiences: []string{c.audience}, ExpirationSeconds: &exp}},
		metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("mint ate-api token (sa %s/%s): %w", c.ns, c.sa, err)
	}
	return map[string]string{"authorization": "Bearer " + tr.Status.Token}, nil
}

func (c *saTokenCreds) RequireTransportSecurity() bool { return true }
