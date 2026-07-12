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
	"strings"
	"time"

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
	defaultActor     = "aei-runtime"
	defaultDNSSuffix = "actors.resources.substrate.ate.dev"
)

// init self-registers the provider; a blank import activates "agent-substrate".
func init() { aei.Register(providerName, build) }

// Options are the dispatch-shaping settings from the provider class parameters.
// ControlURL/BrokerURL are where the runtime calls back (/complete, /redeem);
// unlike a workload provider there is no image — the actor is standing.
type Options struct {
	RouterURL  string
	ActorHost  string
	ControlURL string
	BrokerURL  string
}

func optionsFromConfig(cfg aei.Config) Options {
	host := cfg.Param("actorHost")
	if host == "" {
		host = strings.Join([]string{
			orParam(cfg.Param("actor"), defaultActor),
			orParam(cfg.Param("atespace"), defaultAtespace),
			orParam(cfg.Param("dnsSuffix"), defaultDNSSuffix),
		}, ".")
	}
	return Options{
		RouterURL:  orParam(cfg.Param("routerURL"), defaultRouterURL),
		ActorHost:  host,
		ControlURL: cfg.Param("controlURL"),
		BrokerURL:  cfg.Param("brokerURL"),
	}
}

// Launcher dispatches each run to a durable actor through the atenet-router.
type Launcher struct {
	client *client
	opts   Options
	now    func() time.Time
	log    *slog.Logger
}

var (
	_ aei.Launcher               = (*Launcher)(nil)
	_ aei.AsyncLauncher          = (*Launcher)(nil)
	_ aei.PullCredentialLauncher = (*Launcher)(nil)
)

// New builds a Launcher over the given HTTP client and options.
func New(hc *http.Client, opts Options) *Launcher {
	return &Launcher{client: newClient(opts.RouterURL, hc), opts: opts, now: time.Now}
}

// build constructs the provider. Unlike the Kubernetes providers it needs no
// cluster client, so it never fails to build — it is a pure A2A HTTP client.
func build(cfg aei.Config) (aei.Launcher, error) {
	l := New(&http.Client{}, optionsFromConfig(cfg)) // no client timeout: bounded by the per-run ctx
	l.log = cfg.Logger
	if l.log != nil {
		l.log.Info("using agent-substrate provider (dispatch to a durable actor through the atenet-router; per-run params ride the A2A metadata)",
			"router", l.opts.RouterURL, "actor", l.opts.ActorHost)
	}
	return l, nil
}

// PullsCredential is true: a durable actor's RAM is snapshotted, so a live token
// must never ride the dispatch. The platform mints a single-use ticket instead,
// which the runtime redeems for the token before working (rule R2).
func (l *Launcher) PullsCredential() bool { return true }

// Dispatch sends the run to the durable actor as one A2A message/send through the
// router and returns as soon as the actor accepts it. The per-run AEI_* keys —
// including the redemption ticket — ride the message metadata, because the actor's
// env is frozen in its golden snapshot, so per-run values cannot ride env (rule R1).
func (l *Launcher) Dispatch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	meta := aei.Injection(spec, cred, l.opts.ControlURL, l.opts.BrokerURL, l.now())
	res, err := l.client.sendTask(ctx, l.opts.ActorHost, "Run AEI agent "+spec.ID, meta)
	handle := aei.Handle{Provider: providerName, Ref: res.TaskID}
	if err != nil {
		return handle, fmt.Errorf("agent-substrate: dispatch: %w", err)
	}
	if res.State == stateFailed {
		return handle, fmt.Errorf("agent-substrate: actor rejected run: %s", res.Message)
	}
	return handle, nil
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
