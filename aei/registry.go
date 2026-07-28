package aei

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

// Config is what a Factory receives to build its launcher: provider-specific
// parameters (endpoint, image, namespace, …) plus an optional logger. It mirrors
// AgentProviderClass.parameters (docs/design.md) for the embedded, CRD-less case —
// the same knobs an admin would set on the class.
type Config struct {
	Params map[string]string
	Logger *slog.Logger
}

// Param returns the named parameter, or "" if unset.
func (c Config) Param(name string) string {
	if c.Params == nil {
		return ""
	}
	return c.Params[name]
}

// Factory builds a launcher from configuration. It returns an error for an
// environment the provider cannot run in (e.g. a Kubernetes provider outside a
// cluster), so selection fails fast rather than silently degrading.
type Factory func(cfg Config) (Launcher, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a provider factory under name. It panics on an empty name, a nil
// factory, or a duplicate — each is a registration bug, surfaced at startup
// rather than at dispatch time. Call it from a provider package's init.
//
// This is the database/sql driver pattern: the registry depends only on the
// Launcher interface, so provider client libraries never enter a caller's
// dependency graph. A caller that merely selects a provider by name imports none
// of them; a provider is activated by a blank import, and adding one touches no
// core wiring and no selection switch.
func Register(name string, f Factory) {
	if name == "" || f == nil {
		panic("aei: Register requires a non-empty name and a factory")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic("aei: duplicate registration for provider " + name)
	}
	factories[name] = f
}

// Build constructs the provider named by name. An unknown name is a configuration
// error that lists the registered providers: selection is the swap mechanism, so
// a typo or an unregistered provider must fail fast, never silently fall back.
func Build(name string, cfg Config) (Launcher, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("aei: unknown provider %q (registered: %v)", name, Names())
	}
	return f(cfg)
}

// Names returns the registered provider names, sorted, for diagnostics and the
// unknown-provider error.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for n := range factories {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
