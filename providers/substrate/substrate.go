// Package substrate is an AEI reference provider that dispatches each run to a
// durable Agent Substrate ("ate") actor over A2A, rather than creating a workload
// per run. The actor is a long-lived runtime (the aeiruntime image) that Substrate
// multiplexes and gVisor-checkpoints (suspend/resume); this launcher POSTs one A2A
// message/send per run through the atenet-router, which auto-resumes the actor and
// proxies to it. It is the durable, pull-token bracket: it holds no Kubernetes
// client and needs no AEI RBAC — Substrate owns the actor lifecycle.
//
// It self-registers as "agent-substrate"; activate it with a blank import and
// select it via an AgentProviderClass bound to it. It pulls no third-party module
// (net/http only), so it needs no build tag.
package substrate

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

const (
	providerName = "agent-substrate"
	// stateFailed is the A2A task state the actor returns when it rejects a run
	// synchronously — before running it (e.g. malformed parameters). Any other
	// state means the run was accepted and now runs detached.
	stateFailed = "failed"

	// defaultRouterURL is the in-cluster atenet-router root. It is an FQDN because
	// the controller and ate-system run in different namespaces; the router listens
	// on :80. The actor host is "<actor>.<atespace>.<dnsSuffix>" unless overridden.
	defaultRouterURL = "http://atenet-router.ate-system.svc.cluster.local"
	defaultAtespace  = "aei"
	// defaultActor is both the default "actor" label and the app-less fallback: a
	// run with no origin app (the declarative AgentRun path) pools into this shared
	// actor, matching the runtime-namespace helper's app-less "aei-runtime" pool.
	defaultActor     = "aei-runtime"
	defaultDNSSuffix = "actors.resources.substrate.ate.dev"

	// appToken is the placeholder the "actor"/"actorHost" param uses to opt into a
	// per-app actor. A durable actor is one standing instance of one runtime image
	// (one actor = one app's runtime), so a class whose actor is "{app}" dispatches
	// each app's runs to that app's OWN actor; a literal actor is shared by all apps.
	appToken = "{app}"

	// sessionToken is the placeholder the "actor"/"actorHost" param uses to opt into a
	// per-SESSION actor: a class whose actor is "{app}-{session}" gives each conversation
	// (RunSpec.Session) its OWN standing actor, so the runtime's per-session working set
	// is isolated and warm across turns. A session-less run collapses {session} to the
	// shared pool (defaultActor).
	sessionToken = "{session}"

	// Defaults for AEI-managed actor provisioning (materializing the ActorTemplate
	// and actor from the AgentApp image + admin class params, so the app ships no
	// ate artifacts). The actor serves on :80 — the atenet-router DNATs pod-IP:80 to
	// the actor — so the runtime's AEI_SERVE_ADDR is fixed here, not a class knob.
	defaultTemplateNamespace = "aei-actors"
	defaultSandboxClass      = "gvisor"
	defaultWorkerPool        = "aei"
	defaultPauseImage        = "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

	// ate-api defaults — the gRPC control plane that owns atespaces + actors (not
	// CRDs). They target the in-cluster ate-api-server Service.
	defaultAPIEndpoint = "api.ate-system.svc:443"
	defaultAPIAudience = "api.ate-system.svc"
	defaultAPITokenSA  = "ate-client"
	defaultAPITokenNS  = "ate-system"

	// serveAddr/readyzPath/servePort are the actor's fixed serving contract on :80.
	serveAddr  = ":80"
	readyzPath = "/readyz"
	servePort  = int64(80)
)

// init self-registers the provider; a blank import activates "agent-substrate".
func init() { aei.Register(providerName, build) }

// Options are the dispatch-shaping settings from the provider class parameters.
// ControlURL/BrokerURL are where the runtime calls back (/complete, /redeem);
// unlike a workload provider there is no image — the actor is standing. The actor
// identity is kept as its parts (not a pre-composed host) so it can be resolved per
// run: a {app} template in Actor or ActorHost targets each app's own actor.
type Options struct {
	RouterURL string
	// ActorHost, when set, is a full-host override ("actorHost" param); it wins over
	// the composed form and may itself contain the {app} token.
	ActorHost string
	// Actor is the "actor" label: a literal (a shared actor) or a {app} template (a
	// per-app actor). Composed as "<Actor>.<Atespace>.<DNSSuffix>".
	Actor      string
	Atespace   string
	DNSSuffix  string
	ControlURL string
	BrokerURL  string

	// Actor-provisioning knobs (admin infra). AEI materializes the ActorTemplate
	// from the AgentApp image + these, and the atespace/actor via the ate-api, so an
	// app bound to a durable class ships no ate artifacts. All have safe defaults.
	TemplateNamespace string // k8s namespace holding the ActorTemplate + WorkerPool
	PauseImage        string // pinned pause image (required by the ActorTemplate)
	SnapshotLocation  string // snapshotsConfig.location (required); empty ⇒ skip template ensure
	SandboxClass      string // gvisor (default) | microvm
	AteomImage        string // WorkerPool ateom image; empty ⇒ the admin provides the pool
	WorkerPoolName    string // pool name AEI creates and the template selects
	WorkerReplicas    int64  // pool size when AEI creates it

	// Actor overload controls (all optional; empty ⇒ unset). MaxConcurrentRuns caps
	// how many runs the standing actor executes at once — stamped as
	// AEI_MAX_CONCURRENT_RUNS on the actor, where at capacity the serve loop sheds
	// with a retryable 503 the provider's warm-up retry absorbs (graceful
	// backpressure, not OOM). ActorCPULimit/ActorMemoryLimit are the defense-in-depth
	// resource ceiling beneath it, applied to the AEI-created WorkerPool's pod template
	// (spec.template.resources — ate has no per-container ActorTemplate resources) as
	// opaque Kubernetes quantities ("500m", "512Mi").
	MaxConcurrentRuns string
	ActorCPULimit     string
	ActorMemoryLimit  string

	APIEndpoint string // ate-api gRPC address (default api.ate-system.svc:443)
	APIAudience string // bearer-token audience (default api.ate-system.svc)
	APITokenSA  string // ServiceAccount to mint the bearer for (default ate-client)
	APITokenNS  string // that ServiceAccount's namespace (default ate-system)
}

// optionsFromConfig keeps the actor identity as its parts (Actor/Atespace/DNSSuffix
// or the ActorHost override) rather than composing the host now, so resolveActorHost
// can expand a {app} template against each run's origin app.
func optionsFromConfig(cfg aei.Config) Options {
	replicas := int64(1)
	if v := cfg.Param("workerReplicas"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			replicas = n
		}
	}
	return Options{
		RouterURL:  orParam(cfg.Param("routerURL"), defaultRouterURL),
		ActorHost:  cfg.Param("actorHost"),
		Actor:      orParam(cfg.Param("actor"), defaultActor),
		Atespace:   orParam(cfg.Param("atespace"), defaultAtespace),
		DNSSuffix:  orParam(cfg.Param("dnsSuffix"), defaultDNSSuffix),
		ControlURL: cfg.Param("controlURL"),
		BrokerURL:  cfg.Param("brokerURL"),

		TemplateNamespace: orParam(cfg.Param("templateNamespace"), defaultTemplateNamespace),
		PauseImage:        orParam(cfg.Param("pauseImage"), defaultPauseImage),
		SnapshotLocation:  cfg.Param("snapshotLocation"),
		SandboxClass:      orParam(cfg.Param("sandboxClass"), defaultSandboxClass),
		AteomImage:        cfg.Param("ateomImage"),
		WorkerPoolName:    orParam(cfg.Param("workerPool"), defaultWorkerPool),
		WorkerReplicas:    replicas,

		MaxConcurrentRuns: cfg.Param("maxConcurrentRuns"),
		ActorCPULimit:     cfg.Param("actorCPU"),
		ActorMemoryLimit:  cfg.Param("actorMemory"),

		APIEndpoint: orParam(cfg.Param("ateApiEndpoint"), defaultAPIEndpoint),
		APIAudience: orParam(cfg.Param("ateApiAudience"), defaultAPIAudience),
		APITokenSA:  orParam(cfg.Param("ateApiTokenServiceAccount"), defaultAPITokenSA),
		APITokenNS:  orParam(cfg.Param("ateApiTokenNamespace"), defaultAPITokenNS),
	}
}

// resolveActorHost builds the A2A actor host for one run. Resolving per run makes
// the actor identity per-app: an "actorHost"/"actor" value containing {app} expands
// to the run's origin app (sanitized to a DNS-1123 label); an app-less run falls
// back to the shared defaultActor. A literal actorHost overrides the composed
// "<actor>.<atespace>.<dnsSuffix>" form; a literal actor is shared by all apps.
func (o Options) resolveActorHost(spec aei.RunSpec) string {
	if o.ActorHost != "" {
		return expandTokens(o.ActorHost, spec)
	}
	actor, atespace := o.resolveActor(spec)
	return strings.Join([]string{actor, atespace, o.DNSSuffix}, ".")
}

// resolveActor returns the run's actor name and atespace — the ate-api identity AEI
// provisions and the router routes to — applying the same {app}/{session} expansion as
// resolveActorHost (an app-less or session-less run falls back to the shared
// defaultActor). Because ensure() keys provisioning by the resolved actor, a
// {session} template yields a distinct actor — and thus a distinct golden snapshot —
// per conversation.
func (o Options) resolveActor(spec aei.RunSpec) (actor, atespace string) {
	actor = expandTokens(o.Actor, spec)
	if actor == "" {
		actor = defaultActor
	}
	return actor, o.Atespace
}

// checkIsolation resolves a run's isolation class against what this provider can
// actually satisfy. A durable run executes inside the actor's ate sandbox, so the
// isolation on offer is exactly the class's sandboxClass parameter (gvisor by
// default) — there is no per-run choice, because the actor is standing and shared.
// A run asking for anything else is rejected rather than silently run under a
// different isolation than it named.
func (o Options) checkIsolation(isolation string) error {
	if isolation == "" || isolation == o.SandboxClass {
		return nil
	}
	return fmt.Errorf("isolation class %q: this class's actors run under sandboxClass %q; a durable actor's isolation is fixed at provisioning, not per run", isolation, o.SandboxClass)
}

// actorIdentity is the actor template for logging: the actorHost override if set,
// else the actor label (which may be the {app} per-app template).
func (o Options) actorIdentity() string {
	if o.ActorHost != "" {
		return o.ActorHost
	}
	return o.Actor
}

// expandTokens substitutes the {app} and {session} placeholders in a naming template
// with the run's origin app and session, each sanitized to a DNS-1123 label; an empty
// app or session collapses its token to defaultActor (the shared pool). A template
// with neither token is returned unchanged (a literal, shared host), and only the
// substituted values are sanitized, so a full ActorHost template keeps its dots.
func expandTokens(s string, spec aei.RunSpec) string {
	s = replaceToken(s, appToken, spec.App)
	s = replaceToken(s, sessionToken, spec.Session)
	return s
}

func replaceToken(s, token, value string) string {
	if !strings.Contains(s, token) {
		return s
	}
	label := sanitizeLabel(value)
	if label == "" {
		label = defaultActor
	}
	return strings.ReplaceAll(s, token, label)
}

// nonDNSLabel matches runs of characters not allowed in a DNS-1123 label.
var nonDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

// sanitizeLabel makes s a DNS-1123-label-safe token (the actor is a DNS label in
// the host): lowercased, invalid characters collapsed to '-', trimmed, and
// truncated to 63 characters. It mirrors the runtime-namespace sanitizer so an
// app's actor and namespace derive to the same token.
func sanitizeLabel(s string) string {
	s = nonDNSLabel.ReplaceAllString(strings.ToLower(s), "-")
	s = strings.Trim(s, "-")
	if len(s) > 63 {
		s = strings.Trim(s[:63], "-")
	}
	return s
}

// Launcher dispatches each run to a durable actor through the atenet-router.
type Launcher struct {
	client *client
	opts   Options
	now    func() time.Time
	log    *slog.Logger
	// backoffStart/backoffMax bound the dispatch retry while the actor is warming up.
	backoffStart time.Duration
	backoffMax   time.Duration

	// Cluster wiring for AEI-managed actor provisioning; all nil when off (outside a
	// cluster, or an actorHost override). dyn applies the ate.dev CRDs, kube mints the
	// ate-api bearer, actors creates the atespace + actor (not CRDs) via the ate-api.
	dyn     dynamic.Interface
	kube    kubernetes.Interface
	actors  actorEnsurer
	entry   entrypointResolver // reads an app image's entrypoint for the ActorTemplate command
	ensured sync.Map           // dedupe ensure() per "atespace/actor"
}

var (
	_ aei.Launcher               = (*Launcher)(nil)
	_ aei.AsyncLauncher          = (*Launcher)(nil)
	_ aei.PullCredentialLauncher = (*Launcher)(nil)
	_ aei.Warmer                 = (*Launcher)(nil)
	_ aei.SessionWarmable        = (*Launcher)(nil)
)

// warmupBackoffStart/Max bound the dispatch retry when the router reports the actor
// is not ready (503): a durable actor's first dispatch races its golden-snapshot
// build, and a warm actor may be mid-resume. The provider polls with capped
// exponential backoff (bounded by the run deadline) so an app never has to
// special-case this durable-only transient.
const (
	warmupBackoffStart = 500 * time.Millisecond
	warmupBackoffMax   = 5 * time.Second
)

// New builds a Launcher over the given HTTP client and options.
func New(hc *http.Client, opts Options) *Launcher {
	return &Launcher{
		client: newClient(opts.RouterURL, hc), opts: opts, now: time.Now,
		backoffStart: warmupBackoffStart, backoffMax: warmupBackoffMax,
	}
}

// build constructs the provider. A2A dispatch needs no cluster client, so build
// never fails: it wires the optional actor-provisioning clients best-effort and
// still returns a working dispatcher when they are unavailable (outside a cluster,
// or without RBAC) — then dispatch assumes the actor was provisioned out of band.
func build(cfg aei.Config) (aei.Launcher, error) {
	l := New(&http.Client{}, optionsFromConfig(cfg)) // no client timeout: bounded by the per-run ctx
	l.log = cfg.Logger
	if restCfg, err := rest.InClusterConfig(); err == nil {
		if dyn, e := dynamic.NewForConfig(restCfg); e == nil {
			l.dyn = dyn
		}
		if kube, e := kubernetes.NewForConfig(restCfg); e == nil {
			l.kube = kube
			l.actors = newAteClient(l.opts, kube)
		}
		l.entry = newRegistryResolver(parseRegistryRewrites(os.Getenv("AEI_REGISTRY_REWRITE")))
	} else if l.log != nil {
		l.log.Info("agent-substrate: not in a cluster; actor auto-provisioning is off (dispatch assumes the actor already exists)", "err", err)
	}
	if l.log != nil {
		l.log.Info("using agent-substrate provider (dispatch to a durable actor through the atenet-router; per-run params ride the A2A metadata; the actor identity resolves per run, so a {app} class actor targets each app's own actor)",
			"router", l.opts.RouterURL, "actor", l.opts.actorIdentity(), "atespace", l.opts.Atespace, "autoProvision", l.dyn != nil)
	}
	return l, nil
}

// PullsCredential is true: a durable actor's RAM is snapshotted, so a live token
// must never ride the dispatch. The platform mints a single-use ticket instead,
// which the runtime redeems for the token before working (rule R2).
func (l *Launcher) PullsCredential() bool { return true }

// WarmsSession is true: agent-substrate is a durable serve-loop actor, so the runtime
// SDK's per-session working-set cache survives across a session's turns (warm). A
// class whose actor names include {session} gives each conversation its OWN actor
// (isolation + no shared-cache bound); even a shared actor keeps sessions warm subject
// to the SDK's LRU — either way the substrate is session-warm-capable. Per-run
// providers (k8s-job, agent-sandbox) rebuild per run and do not implement this.
func (l *Launcher) WarmsSession() bool { return true }

// Warm eagerly materializes this app's durable actor (and its golden snapshot) ahead
// of the first run, so the first dispatch does not pay the cold start. It is the same
// idempotent provisioning the dispatch path runs, without the A2A send, and satisfies
// aei.Warmer — which the platform calls when an app binds this provider's class.
func (l *Launcher) Warm(ctx context.Context, spec aei.RunSpec) error {
	return l.ensure(ctx, spec)
}

// Dispatch sends the run to the durable actor as one A2A message/send through the
// router and returns as soon as the actor accepts it. The per-run AEI_* keys —
// including the redemption ticket — ride the message metadata, because the actor's
// env is frozen in its golden snapshot, so per-run values cannot ride env (rule R1).
func (l *Launcher) Dispatch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	if err := l.opts.checkIsolation(spec.Isolation); err != nil {
		return aei.Handle{Provider: providerName}, fmt.Errorf("agent-substrate: %w", err)
	}
	actorHost := l.opts.resolveActorHost(spec)
	if err := l.ensure(ctx, spec); err != nil {
		return aei.Handle{Provider: providerName, Ref: actorHost}, fmt.Errorf("agent-substrate: ensure actor: %w", err)
	}
	meta := aei.Injection(spec, cred, l.opts.ControlURL, l.opts.BrokerURL, l.now())
	res, err := l.sendWithWarmup(ctx, actorHost, "Run AEI agent "+spec.ID, meta)
	handle := aei.Handle{Provider: providerName, Ref: res.TaskID}
	if err != nil {
		return handle, fmt.Errorf("agent-substrate: dispatch: %w", err)
	}
	if res.State == stateFailed {
		return handle, fmt.Errorf("agent-substrate: actor rejected run: %s", res.Message)
	}
	return handle, nil
}

// sendWithWarmup dispatches the task, retrying the router's transient "actor
// unavailable" 503 with capped exponential backoff bounded by ctx (the run deadline).
// That 503 is a durable-substrate cold start — the first dispatch to a new actor
// while its golden snapshot builds, or a warm actor mid-resume — which resolves on
// its own, so the provider absorbs it here: an app programs to one substrate-agnostic
// interface and never special-cases a durable-only transient. Any non-transient error
// (a rejected request, or any other status) returns at once.
func (l *Launcher) sendWithWarmup(ctx context.Context, actorHost, prompt string, meta map[string]string) (taskResult, error) {
	backoff := l.backoffStart
	for attempt := 0; ; attempt++ {
		res, err := l.client.sendTask(ctx, actorHost, prompt, meta)
		if err == nil || !isUnavailable(err) {
			return res, err
		}
		if attempt == 0 && l.log != nil {
			l.log.Info("agent-substrate: actor not ready yet (router 503); retrying dispatch until it resumes or the run deadline elapses",
				"actor", actorHost, "err", err)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return taskResult{}, fmt.Errorf("actor %q did not become ready before the deadline: %w (last: %v)", actorHost, ctx.Err(), err)
		}
		backoff *= 2
		if backoff > l.backoffMax {
			backoff = l.backoffMax
		}
	}
}

// Await is detached: the actor runs the job and reports completion via the runtime
// callback, which the control plane races against this deadline backstop. Reaching
// the deadline (ctx) means no report arrived in time (a timeout).
func (l *Launcher) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

// Launch composes Dispatch and Await for a direct blocking call; an orchestrator
// prefers the split async path so completion is driven by the runtime callback.
func (l *Launcher) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, err := l.Dispatch(ctx, spec, cred)
	if err != nil {
		return h, err
	}
	res, err := l.Await(ctx, h)
	if err != nil {
		return h, err
	}
	return h, res.Err
}

func orParam(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
