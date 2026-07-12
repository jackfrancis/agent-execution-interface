// Package dispatch is the AEI data-plane: the scalable app path that replaces a
// per-run AgentRun object with a single call. An app POSTs /dispatch; the handler
// authenticates the caller's workload identity (TokenReview — no shared secret),
// authorizes it against the AgentApp it dispatches as, mints a bounded run
// credential, and submits through the shared control.Plane. Completion is read
// back with GET /runs/{id}. One AgentApp fronts thousands of runs, so etcd holds
// one object per app, not one per run (docs/dispatch-plane.md).
//
// It lives in the controller module (not control/) because caller authentication
// and class resolution need client-go and the CRD types, which the control plane
// deliberately does not import.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// Caller is the authenticated workload identity of a dispatch request.
type Caller struct{ Namespace, ServiceAccount string }

// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=agentapps,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create

// Authenticator validates a bearer token and returns the caller's workload
// identity. The reference implementation (NewTokenReviewer) calls the Kubernetes
// TokenReview API, so the dispatch endpoint trusts no shared secret.
type Authenticator interface {
	Authenticate(ctx context.Context, bearer string) (Caller, error)
}

// Handler serves the data-plane dispatch API. It holds the shared control plane
// (so /complete callbacks correlate) and resolves a launcher per provider class.
type Handler struct {
	plane  *control.Plane
	auth   Authenticator
	client client.Client

	mu        sync.Mutex
	launchers map[string]aei.Launcher // provider class name -> resolved launcher
	owners    map[string]owner        // runID -> owning app (for status authorization)
}

type owner struct{ Namespace, App string }

// NewHandler builds a dispatch handler over the shared plane, an authenticator,
// and a Kubernetes client (used to read AgentApp / AgentProviderClass).
func NewHandler(plane *control.Plane, auth Authenticator, c client.Client) *Handler {
	return &Handler{
		plane:     plane,
		auth:      auth,
		client:    c,
		launchers: map[string]aei.Launcher{},
		owners:    map[string]owner{},
	}
}

// RegisterRoutes mounts the data-plane routes on mux (alongside the runtime ABI).
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /aei/v1alpha1/dispatch", h.dispatch)
	mux.HandleFunc("GET /aei/v1alpha1/runs/{id}", h.status)
}

// dispatchRequest is the wire body: the AgentRunSpec fields (camelCase — the CRD
// schema, so an app SDK reuses the very same typed model it would for the
// declarative path) plus the target app name.
type dispatchRequest struct {
	App            string            `json:"app"`
	TaskRef        string            `json:"taskRef"`
	Parameters     map[string]string `json:"parameters"`
	TimeoutSeconds int64             `json:"timeoutSeconds"`
	Identity       struct {
		Subject string   `json:"subject"`
		Scopes  []string `json:"scopes"`
	} `json:"identity"`
}

func (h *Handler) dispatch(w http.ResponseWriter, r *http.Request) {
	caller, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req dispatchRequest
	if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req) != nil || req.App == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// The app registration lives in the caller's own namespace.
	var app aeiv1.AgentApp
	if err := h.client.Get(r.Context(), types.NamespacedName{Namespace: caller.Namespace, Name: req.App}, &app); err != nil {
		http.Error(w, "unknown app", http.StatusNotFound)
		return
	}
	// Authorize: the caller SA must be a declared dispatcher of the app.
	if !contains(app.Spec.Dispatchers, caller.ServiceAccount) {
		http.Error(w, "caller not authorized for app", http.StatusForbidden)
		return
	}
	// Least privilege: the run may request no scope outside the app's ceiling.
	scopes, okScopes := boundScopes(req.Identity.Scopes, app.Spec.Identity.MaxScopes)
	if !okScopes {
		http.Error(w, "requested scope exceeds app policy", http.StatusForbidden)
		return
	}

	launcher, err := h.launcherFor(r.Context(), app.Spec.ProviderClassName)
	if err != nil {
		http.Error(w, "provider unavailable", http.StatusBadGateway)
		return
	}

	spec := aei.RunSpec{
		TaskRef:    req.TaskRef,
		Parameters: req.Parameters,
		Identity: aei.IdentityRequest{
			Subject:  req.Identity.Subject,
			Scopes:   scopes,
			Audience: app.Spec.Identity.Audience, // the app fixes the audience
		},
		Deadline: time.Duration(req.TimeoutSeconds) * time.Second,
	}
	runID, err := h.plane.Submit(r.Context(), launcher, spec)
	if err != nil {
		http.Error(w, "dispatch failed", http.StatusInternalServerError)
		return
	}
	h.setOwner(runID, owner{Namespace: caller.Namespace, App: req.App})
	writeJSON(w, http.StatusAccepted, map[string]any{"runId": runID})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	caller, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	o, known := h.ownerOf(id)
	if !known {
		http.Error(w, "unknown run", http.StatusNotFound)
		return
	}
	// Authorize: only a dispatcher of the owning app may read its runs.
	var app aeiv1.AgentApp
	if err := h.client.Get(r.Context(), types.NamespacedName{Namespace: o.Namespace, Name: o.App}, &app); err != nil ||
		caller.Namespace != o.Namespace || !contains(app.Spec.Dispatchers, caller.ServiceAccount) {
		http.Error(w, "caller not authorized for run", http.StatusForbidden)
		return
	}
	st, found := h.plane.Status(id)
	if !found {
		http.Error(w, "unknown run", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"phase":     st.Phase,
		"message":   st.Err,
		"resultRef": st.ResultRef,
		"timing": map[string]any{
			"provisioningSeconds": st.Timing.ProvisioningSeconds,
			"runtimeSeconds":      st.Timing.RuntimeSeconds,
			"modelSeconds":        st.Timing.ModelSeconds,
			"modelCalls":          st.Timing.ModelCalls,
			"toolCalls":           st.Timing.ToolCalls,
		},
	})
}

func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (Caller, bool) {
	bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Caller{}, false
	}
	caller, err := h.auth.Authenticate(r.Context(), bearer)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Caller{}, false
	}
	return caller, true
}

// launcherFor resolves (and caches) the launcher bound to a provider class.
// Mirrors the reconciler's class resolution — the two dispatch ingresses share
// the same providers, just reached from a watch vs a request.
func (h *Handler) launcherFor(ctx context.Context, className string) (aei.Launcher, error) {
	class, err := h.resolveClass(ctx, className)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.launchers[class.Name]; ok {
		return l, nil
	}
	l, err := aei.Build(class.Spec.Provider, aei.Config{Params: class.Spec.Parameters})
	if err != nil {
		return nil, err
	}
	h.launchers[class.Name] = l
	return l, nil
}

func (h *Handler) resolveClass(ctx context.Context, name string) (*aeiv1.AgentProviderClass, error) {
	if name != "" {
		var c aeiv1.AgentProviderClass
		if err := h.client.Get(ctx, types.NamespacedName{Name: name}, &c); err != nil {
			return nil, err
		}
		return &c, nil
	}
	var list aeiv1.AgentProviderClassList
	if err := h.client.List(ctx, &list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Annotations[aeiv1.IsDefaultClassAnnotation] == "true" {
			return &list.Items[i], nil
		}
	}
	return nil, errors.New("no providerClassName and no default AgentProviderClass")
}

func (h *Handler) setOwner(runID string, o owner) {
	h.mu.Lock()
	h.owners[runID] = o
	h.mu.Unlock()
}

func (h *Handler) ownerOf(runID string) (owner, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o, ok := h.owners[runID]
	return o, ok
}

// --- TokenReview authenticator (the no-shared-secret caller identity) ---

type tokenReviewer struct{ client kubernetes.Interface }

// NewTokenReviewer authenticates callers by submitting their token to the
// Kubernetes TokenReview API and extracting the ServiceAccount identity.
func NewTokenReviewer(c kubernetes.Interface) Authenticator { return &tokenReviewer{client: c} }

func (t *tokenReviewer) Authenticate(ctx context.Context, token string) (Caller, error) {
	tr, err := t.client.AuthenticationV1().TokenReviews().Create(ctx,
		&authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token}}, metav1.CreateOptions{})
	if err != nil {
		return Caller{}, err
	}
	if !tr.Status.Authenticated {
		return Caller{}, errors.New("dispatch: token not authenticated")
	}
	ns, name, ok := parseServiceAccount(tr.Status.User.Username)
	if !ok {
		return Caller{}, errors.New("dispatch: caller is not a service account")
	}
	return Caller{Namespace: ns, ServiceAccount: name}, nil
}

// parseServiceAccount splits "system:serviceaccount:<ns>:<name>".
func parseServiceAccount(username string) (ns, name string, ok bool) {
	const prefix = "system:serviceaccount:"
	rest, found := strings.CutPrefix(username, prefix)
	if !found {
		return "", "", false
	}
	return strings.Cut(rest, ":")
}

// --- helpers ---

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// boundScopes returns requested ∩ max and whether every requested scope was
// within max (least privilege: a request may narrow, never widen).
func boundScopes(requested, max []string) ([]string, bool) {
	out := make([]string, 0, len(requested))
	for _, s := range requested {
		if !contains(max, s) {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
