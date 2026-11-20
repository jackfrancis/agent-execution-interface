package dispatch

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/jackfrancis/agent-execution-interface/control"
)

// apiserverAudience stands in for the apiserver's own audience: the one a pod's
// default ServiceAccount token carries, and the one a TokenReview with no
// audiences checks against.
const apiserverAudience = "https://kubernetes.default.svc.cluster.local"

// fakeAPIServer answers TokenReviews the way the apiserver does. Each token maps
// to the audiences it was minted for and the ServiceAccount it names. A review
// with spec.audiences authenticates a token that carries at least one of them and
// reports the overlap in status.audiences; a review without them checks the token
// against the apiserver's own audience.
type fakeAPIServer struct {
	mu      sync.Mutex
	tokens  map[string]fakeToken
	reviews [][]string // spec.audiences of each review, in order
	err     error      // when set, every review fails with it
}

type fakeToken struct {
	audiences []string
	user      string
}

func newFakeAPIServer() *fakeAPIServer {
	return &fakeAPIServer{tokens: map[string]fakeToken{
		"dispatch-token": {[]string{DefaultAudience}, "system:serviceaccount:team:pyfer"},
		"legacy-token":   {[]string{apiserverAudience}, "system:serviceaccount:team:pyfer"},
		"other-token":    {[]string{"some-other-service"}, "system:serviceaccount:team:pyfer"},
		"user-token":     {[]string{DefaultAudience}, "alice"},
	}}
}

func (f *fakeAPIServer) clientset() *k8sfake.Clientset {
	cs := k8sfake.NewClientset()
	cs.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview).DeepCopy()
		f.mu.Lock()
		defer f.mu.Unlock()
		f.reviews = append(f.reviews, tr.Spec.Audiences)
		if f.err != nil {
			return true, nil, f.err
		}
		tok, known := f.tokens[tr.Spec.Token]
		want := tr.Spec.Audiences
		if len(want) == 0 {
			want = []string{apiserverAudience}
		}
		var overlap []string
		for _, a := range want {
			if slices.Contains(tok.audiences, a) {
				overlap = append(overlap, a)
			}
		}
		if known && len(overlap) > 0 {
			tr.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: overlap,
				User: authnv1.UserInfo{Username: tok.user}}
		} else {
			tr.Status = authnv1.TokenReviewStatus{Error: "token audiences are invalid for the target audiences"}
		}
		return true, tr, nil
	})
	return cs
}

// TestTokenReviewRequestsDispatchAudience: the first review always names the
// dispatch audience, so the apiserver checks the token against it.
func TestTokenReviewRequestsDispatchAudience(t *testing.T) {
	f := newFakeAPIServer()
	auth := NewTokenReviewer(f.clientset(), "")
	if _, err := auth.Authenticate(context.Background(), "dispatch-token"); err != nil {
		t.Fatalf("dispatch-audience token: %v", err)
	}
	if got := f.reviews; len(got) != 1 || !slices.Equal(got[0], []string{DefaultAudience}) {
		t.Fatalf("reviews = %v, want one for [%s]", got, DefaultAudience)
	}
}

// TestTokenReviewAudiences covers each kind of token: only one minted for the
// dispatch audience and naming a ServiceAccount authenticates, in one review. A
// pod's default token (the apiserver's audience) is not retried another way.
func TestTokenReviewAudiences(t *testing.T) {
	cases := []struct {
		token  string
		wantOK bool
	}{
		{"dispatch-token", true},
		{"legacy-token", false},
		{"other-token", false},
		{"unknown-token", false},
		{"user-token", false}, // authenticated, but not a ServiceAccount
	}
	for _, tc := range cases {
		t.Run(tc.token, func(t *testing.T) {
			f := newFakeAPIServer()
			auth := NewTokenReviewer(f.clientset(), "")
			c, err := auth.Authenticate(context.Background(), tc.token)
			if tc.wantOK != (err == nil) {
				t.Fatalf("Authenticate(%s) err = %v, want ok=%v", tc.token, err, tc.wantOK)
			}
			if tc.wantOK && c != (Caller{Namespace: "team", ServiceAccount: "pyfer"}) {
				t.Errorf("caller = %+v", c)
			}
			if got := f.reviews; len(got) != 1 || !slices.Equal(got[0], []string{DefaultAudience}) {
				t.Errorf("reviews = %v, want one for [%s]", got, DefaultAudience)
			}
		})
	}
}

// TestTokenReviewChecksStatusAudiences: an apiserver answer that authenticates the
// token but does not list the dispatch audience is not enough. TokenReview leaves
// that check to the client.
func TestTokenReviewChecksStatusAudiences(t *testing.T) {
	cs := k8sfake.NewClientset()
	cs.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview).DeepCopy()
		tr.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: []string{apiserverAudience},
			User: authnv1.UserInfo{Username: "system:serviceaccount:team:pyfer"}}
		return true, tr, nil
	})
	auth := NewTokenReviewer(cs, "")
	if _, err := auth.Authenticate(context.Background(), "any"); err == nil {
		t.Fatal("authenticated without the dispatch audience in status.audiences")
	}
}

// TestTokenReviewCustomAudience: the audience is configurable, and the default
// one is then no longer accepted.
func TestTokenReviewCustomAudience(t *testing.T) {
	f := newFakeAPIServer()
	f.tokens["custom-token"] = fakeToken{[]string{"my-dispatch"}, "system:serviceaccount:team:pyfer"}
	auth := NewTokenReviewer(f.clientset(), "my-dispatch")
	if _, err := auth.Authenticate(context.Background(), "custom-token"); err != nil {
		t.Fatalf("custom-audience token: %v", err)
	}
	if _, err := auth.Authenticate(context.Background(), "dispatch-token"); err == nil {
		t.Fatal("token for the default audience accepted under a custom audience")
	}
}

// TestTokenReviewError: when the apiserver call itself fails, the error comes back
// as is, so the handler can answer 503 at its deadline.
func TestTokenReviewError(t *testing.T) {
	f := newFakeAPIServer()
	f.err = errors.New("apiserver unavailable")
	auth := NewTokenReviewer(f.clientset(), "")
	if _, err := auth.Authenticate(context.Background(), "dispatch-token"); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("err = %v, want the apiserver error", err)
	}
}

// TestDispatchAudienceOverHTTP drives /dispatch with the TokenReview authenticator:
// a dispatch-audience token dispatches, and a token for another audience, or a
// pod's default token, is refused with 401 before any run starts.
func TestDispatchAudienceOverHTTP(t *testing.T) {
	f := newFakeAPIServer()
	srv := newTestServerAuth(t, nil, control.Config{Secret: []byte("k")}, NewTokenReviewer(f.clientset(), ""))
	url := srv.URL + "/aei/v1alpha1/dispatch"
	body := dispatchRequest{App: "pyfer", TaskRef: "item://hello", TimeoutSeconds: 1}

	for _, tc := range []struct {
		token string
		code  int
	}{
		{"other-token", http.StatusUnauthorized},
		{"legacy-token", http.StatusUnauthorized},
		{"dispatch-token", http.StatusAccepted},
	} {
		resp := do(t, http.MethodPost, url, tc.token, body)
		_ = resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Fatalf("%s: status = %d, want %d", tc.token, resp.StatusCode, tc.code)
		}
	}
	// Only the accepted dispatch reached the provider.
	select {
	case <-testLauncher.dispatched:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch-token: accepted, but no run reached the provider")
	}
	select {
	case <-testLauncher.dispatched:
		t.Fatal("a refused dispatch reached the provider")
	case <-time.After(100 * time.Millisecond):
	}
}
