package substrate

import (
	"testing"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestResolveActorHost covers the per-app actor identity: a {app} template targets
// each app's own actor, an app-less run pools into the shared default, and a literal
// actor (or actorHost override) stays shared.
func TestResolveActorHost(t *testing.T) {
	const suffix = "aei.actors.resources.substrate.ate.dev" // defaultAtespace.defaultDNSSuffix

	cases := []struct {
		name    string
		params  map[string]string
		app     string
		session string
		want    string
	}{
		{
			name:   "per-app template resolves to the app",
			params: map[string]string{"actor": "{app}"},
			app:    "gofer",
			want:   "gofer." + suffix,
		},
		{
			name:   "per-app template, app-less run pools into the shared default actor",
			params: map[string]string{"actor": "{app}"},
			app:    "",
			want:   "aei-runtime." + suffix,
		},
		{
			name:   "literal actor is shared across apps",
			params: map[string]string{"actor": "aei-runtime"},
			app:    "gofer",
			want:   "aei-runtime." + suffix,
		},
		{
			name:   "default actor (no param) is the shared pool",
			params: nil,
			app:    "gofer",
			want:   "aei-runtime." + suffix,
		},
		{
			name:   "template with suffix keeps the suffix and sanitizes the app",
			params: map[string]string{"actor": "{app}-runtime"},
			app:    "Gofer",
			want:   "gofer-runtime." + suffix,
		},
		{
			name:   "app is sanitized to a DNS-1123 label",
			params: map[string]string{"actor": "{app}"},
			app:    "My App!",
			want:   "my-app." + suffix,
		},
		{
			name:   "custom atespace and dnsSuffix compose",
			params: map[string]string{"actor": "{app}", "atespace": "prod", "dnsSuffix": "x.example.com"},
			app:    "gofer",
			want:   "gofer.prod.x.example.com",
		},
		{
			name:   "actorHost override wins and ignores the composed form",
			params: map[string]string{"actorHost": "custom.example.com", "actor": "{app}"},
			app:    "gofer",
			want:   "custom.example.com",
		},
		{
			name:   "actorHost override still expands {app} and keeps its dots",
			params: map[string]string{"actorHost": "{app}.pool.example.com"},
			app:    "gofer",
			want:   "gofer.pool.example.com",
		},
		{
			name:    "per-session template resolves each conversation to its own actor",
			params:  map[string]string{"actor": "{app}-{session}"},
			app:     "gofer",
			session: "T123",
			want:    "gofer-t123." + suffix,
		},
		{
			name:    "per-session template, session-less run pools into the shared default",
			params:  map[string]string{"actor": "{app}-{session}"},
			app:     "gofer",
			session: "",
			want:    "gofer-aei-runtime." + suffix,
		},
		{
			name:    "session-only template targets the session actor",
			params:  map[string]string{"actor": "{session}"},
			session: "T123",
			want:    "t123." + suffix,
		},
		{
			name:    "session is sanitized to a DNS-1123 label",
			params:  map[string]string{"actor": "{session}"},
			session: "Thread #7",
			want:    "thread-7." + suffix,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := optionsFromConfig(aei.Config{Params: tc.params})
			if got := opts.resolveActorHost(aei.RunSpec{App: tc.app, Session: tc.session}); got != tc.want {
				t.Errorf("resolveActorHost(app=%q) = %q, want %q", tc.app, got, tc.want)
			}
		})
	}
}

// TestWarmsSession: agent-substrate declares the SessionWarmable capability (it is a
// durable serve-loop actor), so a caller can feature-detect warm session affinity.
func TestWarmsSession(t *testing.T) {
	var l aei.Launcher = New(nil, Options{})
	sw, ok := l.(aei.SessionWarmable)
	if !ok {
		t.Fatal("substrate.Launcher must implement aei.SessionWarmable")
	}
	if !sw.WarmsSession() {
		t.Error("agent-substrate should report WarmsSession() == true")
	}
}

// TestOptionsFromConfigDefaults: an empty config yields the actor parts (not a
// pre-composed host), so resolveActorHost can expand a template per run.
func TestOptionsFromConfigDefaults(t *testing.T) {
	opts := optionsFromConfig(aei.Config{})
	if opts.RouterURL != defaultRouterURL {
		t.Errorf("RouterURL = %q, want %q", opts.RouterURL, defaultRouterURL)
	}
	if opts.Actor != defaultActor {
		t.Errorf("Actor = %q, want %q", opts.Actor, defaultActor)
	}
	if opts.Atespace != defaultAtespace || opts.DNSSuffix != defaultDNSSuffix {
		t.Errorf("atespace/dnsSuffix = %q/%q, want %q/%q", opts.Atespace, opts.DNSSuffix, defaultAtespace, defaultDNSSuffix)
	}
	if opts.ActorHost != "" {
		t.Errorf("ActorHost = %q, want empty (compose from parts)", opts.ActorHost)
	}
	if id := opts.actorIdentity(); id != defaultActor {
		t.Errorf("actorIdentity = %q, want %q", id, defaultActor)
	}
}

// TestCheckIsolation: a durable actor's isolation is fixed at provisioning, so the
// provider accepts only the class's own sandboxClass (or no request at all) and
// rejects anything else instead of running under isolation the app did not name.
func TestCheckIsolation(t *testing.T) {
	opts := Options{SandboxClass: defaultSandboxClass}
	for _, tc := range []struct {
		isolation string
		wantErr   bool
	}{
		{"", false},
		{"gvisor", false},
		{"microvm", true},
		{"kata", true},
	} {
		err := opts.checkIsolation(tc.isolation)
		if (err != nil) != tc.wantErr {
			t.Errorf("checkIsolation(%q) error = %v, wantErr %v", tc.isolation, err, tc.wantErr)
		}
	}
	if err := (Options{SandboxClass: "microvm"}).checkIsolation("microvm"); err != nil {
		t.Errorf("a class configured for microvm should satisfy microvm: %v", err)
	}
}

