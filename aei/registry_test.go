package aei_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// stubLauncher is a minimal Launcher used across the registry and capability
// tests. It implements only the required method.
type stubLauncher struct{}

func (stubLauncher) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{Provider: "stub"}, nil
}

// asyncStub adds the AsyncLauncher capability on top of the required core, so the
// feature-detection test can assert a positive and a negative in one type.
type asyncStub struct{ stubLauncher }

func (asyncStub) Dispatch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (asyncStub) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{}, nil
}

func TestRegisterAndBuild(t *testing.T) {
	var built bool
	aei.Register("test-build", func(aei.Config) (aei.Launcher, error) {
		built = true
		return stubLauncher{}, nil
	})

	l, err := aei.Build("test-build", aei.Config{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if l == nil || !built {
		t.Fatalf("factory not invoked or returned nil (built=%v)", built)
	}
}

// TestBuildUnknownNameListsRegistered verifies selection fails fast on an unknown
// provider and the error names what *is* registered, to aid diagnosis.
func TestBuildUnknownNameListsRegistered(t *testing.T) {
	aei.Register("test-known", func(aei.Config) (aei.Launcher, error) {
		return stubLauncher{}, nil
	})
	_, err := aei.Build("test-nope", aei.Config{})
	if err == nil {
		t.Fatal("expected an error for an unregistered provider")
	}
	if !strings.Contains(err.Error(), "test-known") {
		t.Fatalf("error should list registered providers, got %q", err)
	}
}

// TestBuildPropagatesFactoryError verifies a provider that cannot run in this
// environment surfaces its error rather than being silently skipped.
func TestBuildPropagatesFactoryError(t *testing.T) {
	want := errors.New("no cluster")
	aei.Register("test-err", func(aei.Config) (aei.Launcher, error) {
		return nil, want
	})
	if _, err := aei.Build("test-err", aei.Config{}); !errors.Is(err, want) {
		t.Fatalf("expected the factory error to propagate, got %v", err)
	}
}

func TestConfigParam(t *testing.T) {
	cfg := aei.Config{Params: map[string]string{"endpoint": "http://x"}}
	if got := cfg.Param("endpoint"); got != "http://x" {
		t.Fatalf("Param(endpoint) = %q, want http://x", got)
	}
	if got := (aei.Config{}).Param("missing"); got != "" {
		t.Fatalf("Param on nil map = %q, want empty", got)
	}
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	aei.Register("test-dup", func(aei.Config) (aei.Launcher, error) { return stubLauncher{}, nil })
	assertPanics(t, "duplicate registration", func() {
		aei.Register("test-dup", func(aei.Config) (aei.Launcher, error) { return stubLauncher{}, nil })
	})
}

func TestRegisterRejectsEmptyNameAndNilFactory(t *testing.T) {
	assertPanics(t, "empty name", func() {
		aei.Register("", func(aei.Config) (aei.Launcher, error) { return stubLauncher{}, nil })
	})
	assertPanics(t, "nil factory", func() { aei.Register("test-nil-factory", nil) })
}

// TestCapabilityFeatureDetection documents the capability model: an orchestrator
// discovers optional capabilities with a type assertion, not a flag.
func TestCapabilityFeatureDetection(t *testing.T) {
	var plain aei.Launcher = stubLauncher{}
	if _, ok := plain.(aei.AsyncLauncher); ok {
		t.Fatal("stubLauncher must not satisfy AsyncLauncher")
	}

	var async aei.Launcher = asyncStub{}
	if _, ok := async.(aei.AsyncLauncher); !ok {
		t.Fatal("asyncStub must satisfy AsyncLauncher")
	}
	if _, ok := async.(aei.PullCredentialLauncher); ok {
		t.Fatal("asyncStub must not satisfy PullCredentialLauncher")
	}
}

func assertPanics(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected a panic: %s", what)
		}
	}()
	fn()
}
