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
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

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
// TokenReview API for the dispatch audience, so the dispatch endpoint trusts no
// shared secret and no token minted for another service.
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
	launchers map[string]cachedLauncher // provider class name -> resolved launcher
	owners    map[string]owner          // runID -> owning app (for status authorization)
}

type owner struct{ Namespace, App string }

// cachedLauncher is a resolved launcher and the provider driver it was built
// from. The driver is kept with the launcher so a run is attributed to the driver
// that actually dispatches it, even if the class has since been edited.
type cachedLauncher struct {
	launcher aei.Launcher
	driver   string
}

// overloadRetryAfter is the Retry-After (seconds) sent with a 429 when the control
// plane's dispatch queue is full. Creates take well under a second each, so a
// short back-off is enough for the queue to drain.
const overloadRetryAfter = "1"

// NewHandler builds a dispatch handler over the shared plane, an authenticator,
// and a Kubernetes client (used to read AgentApp / AgentProviderClass).
func NewHandler(plane *control.Plane, auth Authenticator, c client.Client) *Handler {
	return &Handler{
		plane:     plane,
		auth:      auth,
		client:    c,
		launchers: map[string]cachedLauncher{},
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
	Session        string            `json:"session"`
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
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		// The listener's read timeout runs from the start of the request, so a slow
		// authentication can leave too little of it for the body. That is a
		// retryable 408, not a malformed request.
		if isReadTimeout(err) {
			http.Error(w, "request body timed out", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.App == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// The app registration lives in the caller's own namespace.
	var app aeiv1.AgentApp
	if err := h.client.Get(r.Context(), types.NamespacedName{Namespace: caller.Namespace, Name: req.App}, &app); err != nil {
		if timedOut(w, r) {
			return
		}
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

	class, err := h.resolveClass(r.Context(), app.Spec.ProviderClassName)
	if err != nil {
		if timedOut(w, r) {
			return
		}
		http.Error(w, "provider class unavailable", http.StatusBadGateway)
		return
	}
	// Isolation is the admin's policy on the class, so it applies to every run the
	// app dispatches — resolve and admit it here, or the app silently runs under the
	// default runtime while its class advertises isolation.
	isolation := class.ResolveIsolation("")
	if err := class.CheckIsolation(isolation, ""); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	launcher, driver, err := h.launcherForClass(class)
	if err != nil {
		http.Error(w, "provider unavailable", http.StatusBadGateway)
		return
	}

	spec := aei.RunSpec{
		TaskRef:      req.TaskRef,
		Image:        app.Spec.Image,
		Parameters:   req.Parameters,
		App:          req.App,
		AppNamespace: caller.Namespace,
		Session:      req.Session,
		Isolation:    isolation,
		Identity: aei.IdentityRequest{
			Subject:  req.Identity.Subject,
			Scopes:   scopes,
			Audience: app.Spec.Identity.Audience, // the app fixes the audience
		},
		Deadline: control.DeadlineFromSeconds(req.TimeoutSeconds),
	}
	// The 202 and its run id must reach the caller, or the caller's retry launches
	// a second run. If the lookups above used up the request's deadline (the
	// listener sets one shorter than its write timeout), refuse before launching.
	if timedOut(w, r) {
		return
	}
	runID, err := h.plane.Submit(r.Context(), launcher, spec, control.WithProvider(driver), control.WithProviderClass(class.Name))
	if errors.Is(err, control.ErrOverloaded) {
		// At the dispatch bound: refuse at once so the caller can back off and
		// retry, rather than queueing without limit. Nothing was recorded.
		w.Header().Set("Retry-After", overloadRetryAfter)
		http.Error(w, "dispatch queue full", http.StatusTooManyRequests)
		return
	}
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
	err := h.client.Get(r.Context(), types.NamespacedName{Namespace: o.Namespace, Name: o.App}, &app)
	if err != nil && timedOut(w, r) {
		return
	}
	if err != nil || caller.Namespace != o.Namespace || !contains(app.Spec.Dispatchers, caller.ServiceAccount) {
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
		if timedOut(w, r) {
			return Caller{}, false
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Caller{}, false
	}
	return caller, true
}

// isReadTimeout reports whether err is the connection's read deadline expiring.
func isReadTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// timedOut answers 503 when the request's deadline has passed. The listener sets
// that deadline shorter than its write timeout, so a lookup it cuts short is
// reported as retryable, not as the lookup's own verdict (401, 403, 404), and a
// dispatch stops before launching a run whose response could not be delivered.
// Nothing has changed when it answers, so the caller can safely retry.
func timedOut(w http.ResponseWriter, r *http.Request) bool {
	if r.Context().Err() == nil {
		return false
	}
	http.Error(w, "request deadline exceeded; nothing was started, retry", http.StatusServiceUnavailable)
	return true
}

// launcherForClass builds (and caches) the launcher bound to a resolved provider
// class, and returns the provider driver it was built from. Mirrors the
// reconciler's launcherFor — the two dispatch ingresses share the same providers,
// just reached from a watch vs a request.
func (h *Handler) launcherForClass(class *aeiv1.AgentProviderClass) (aei.Launcher, string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.launchers[class.Name]; ok {
		return c.launcher, c.driver, nil
	}
	l, err := aei.Build(class.Spec.Provider, aei.Config{Params: class.Spec.Parameters})
	if err != nil {
		return nil, "", err
	}
	h.launchers[class.Name] = cachedLauncher{launcher: l, driver: class.Spec.Provider}
	return l, class.Spec.Provider, nil
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

// DefaultAudience is the token audience the dispatch API requires. An app projects
// a ServiceAccount token for it, so a token minted for another service (or the
// pod's default token, which the apiserver accepts) cannot dispatch.
const DefaultAudience = "aei-dispatch"

type tokenReviewer struct {
	client   kubernetes.Interface
	audience string
}

// NewTokenReviewer authenticates callers by submitting their token to the
// Kubernetes TokenReview API for audience (empty means DefaultAudience) and
// extracting the ServiceAccount identity. Only a token minted for that audience
// authenticates: a pod's default ServiceAccount token, which every pod has unless
// it opts out, cannot dispatch.
func NewTokenReviewer(c kubernetes.Interface, audience string) Authenticator {
	if audience == "" {
		audience = DefaultAudience
	}
	return &tokenReviewer{client: c, audience: audience}
}

func (t *tokenReviewer) Authenticate(ctx context.Context, token string) (Caller, error) {
	tr, err := t.review(ctx, token, []string{t.audience})
	if err != nil {
		return Caller{}, err
	}
	// The apiserver answers with the requested audiences the token is valid for;
	// TokenReview's contract leaves it to the client to check that list.
	if !tr.Status.Authenticated || !contains(tr.Status.Audiences, t.audience) {
		return Caller{}, errors.New("dispatch: token not authenticated for audience " + t.audience)
	}
	return callerOf(tr)
}

func (t *tokenReviewer) review(ctx context.Context, token string, audiences []string) (*authnv1.TokenReview, error) {
	return t.client.AuthenticationV1().TokenReviews().Create(ctx,
		&authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token, Audiences: audiences}}, metav1.CreateOptions{})
}

func callerOf(tr *authnv1.TokenReview) (Caller, error) {
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
