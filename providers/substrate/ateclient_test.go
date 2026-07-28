package substrate

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ateapipb "github.com/jackfrancis/agent-execution-interface/providers/substrate/internal/ateapipb"
)

// fakeControlServer is an in-memory ate-api Control server for the client adapter
// tests: it records creations and can simulate an AlreadyExists.
type fakeControlServer struct {
	ateapipb.UnimplementedControlServer
	spaces  []string
	actors  []string
	already bool
}

func (s *fakeControlServer) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	if s.already {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	s.spaces = append(s.spaces, req.GetAtespace().GetMetadata().GetName())
	return req.GetAtespace(), nil
}

func (s *fakeControlServer) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if s.already {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	a := req.GetActor()
	s.actors = append(s.actors, a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName()+"<-"+a.GetActorTemplateNamespace()+"/"+a.GetActorTemplateName())
	return a, nil
}

// dialFake serves fakeControlServer over an in-memory bufconn and returns an
// ateClient whose ControlClient is wired to it (bypassing the TLS/token dial).
func dialFake(t *testing.T, srv *fakeControlServer) *ateClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	ateapipb.RegisterControlServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &ateClient{ctrl: ateapipb.NewControlClient(conn)}
}

// TestAteClientEnsureCreatesAtespaceAndActor: the adapter sends the atespace/actor
// creations with the fields AEI sets (atespace, name, template ref).
func TestAteClientEnsureCreatesAtespaceAndActor(t *testing.T) {
	srv := &fakeControlServer{}
	a := dialFake(t, srv)

	if err := a.EnsureAtespace(context.Background(), "aei"); err != nil {
		t.Fatalf("EnsureAtespace: %v", err)
	}
	if err := a.EnsureActor(context.Background(), "aei", "gofer", "aei-actors", "gofer"); err != nil {
		t.Fatalf("EnsureActor: %v", err)
	}
	if len(srv.spaces) != 1 || srv.spaces[0] != "aei" {
		t.Errorf("spaces = %v, want [aei]", srv.spaces)
	}
	want := "aei/gofer<-aei-actors/gofer"
	if len(srv.actors) != 1 || srv.actors[0] != want {
		t.Errorf("actors = %v, want [%s]", srv.actors, want)
	}
}

// TestAteClientToleratesAlreadyExists: a re-ensure of an existing atespace/actor is
// success (idempotent).
func TestAteClientToleratesAlreadyExists(t *testing.T) {
	srv := &fakeControlServer{already: true}
	a := dialFake(t, srv)

	if err := a.EnsureAtespace(context.Background(), "aei"); err != nil {
		t.Errorf("AlreadyExists atespace should be tolerated: %v", err)
	}
	if err := a.EnsureActor(context.Background(), "aei", "gofer", "aei-actors", "gofer"); err != nil {
		t.Errorf("AlreadyExists actor should be tolerated: %v", err)
	}
}
