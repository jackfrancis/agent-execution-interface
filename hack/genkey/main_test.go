package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

func TestPublicFromPrivate(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	enc := base64.StdEncoding.EncodeToString

	got, err := publicFromPrivate(strings.NewReader(enc(priv) + "\n"))
	if err != nil || got != enc(pub) {
		t.Fatalf("publicFromPrivate = %q, %v; want %q", got, err, enc(pub))
	}

	for name, in := range map[string]string{
		"not base64":         "!!!",
		"wrong size":         enc([]byte("short")),
		"mismatched half":    enc(append(append([]byte{}, priv.Seed()...), otherPub...)),
		"empty":              "",
		"public key instead": enc(pub),
	} {
		if got, err := publicFromPrivate(strings.NewReader(in)); err == nil {
			t.Errorf("%s: derived %q; want an error", name, got)
		}
	}
}
