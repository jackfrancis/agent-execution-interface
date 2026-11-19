// Command genkey generates a platform Ed25519 keypair for the AEI control plane's
// asymmetric run-credential authority (docs/design.md, Contract 3). The control
// plane is the sole minter: it holds the private key (AEI_ED25519_PRIVATE_KEY) and
// a verify-only app holds only the public half — no shared secret, and an app can
// never mint. `make authority` runs this once and stores the keys in the cluster.
//
// Both values are base64 (StdEncoding): the private key is the 64-byte Ed25519
// private key the controller decodes for AEI_ED25519_PRIVATE_KEY; the public key
// is the 32-byte verification key an app uses (also served at GET
// /aei/v1alpha1/authority).
//
// With -public-from-private it instead reads such a base64 private key on stdin
// (stdin, so the key never appears on a command line) and prints the public key
// that verifies it. It first checks that the key's embedded public half matches its
// seed, the same check the controller makes at startup; `make authority` uses this
// to advertise the key the controller actually signs with.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	fromPrivate := flag.Bool("public-from-private", false, "read a base64 Ed25519 private key on stdin and print its public key")
	flag.Parse()
	if *fromPrivate {
		pub, err := publicFromPrivate(os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, "genkey:", err)
			os.Exit(1)
		}
		fmt.Println(pub)
		return
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fmt.Fprintln(os.Stderr, "genkey:", err)
		os.Exit(1)
	}
	fmt.Printf("AEI_ED25519_PRIVATE_KEY=%s\n", base64.StdEncoding.EncodeToString(priv))
	fmt.Printf("AEI_ED25519_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(pub))
}

// publicFromPrivate reads a base64 (StdEncoding) Ed25519 private key and returns
// its base64 public key. A Go private key is seed || public key; signing uses both
// halves, so one whose second half does not match its seed mints signatures no
// public key verifies, and is rejected rather than "derived".
func publicFromPrivate(r io.Reader) (string, error) {
	in, err := io.ReadAll(io.LimitReader(r, 1<<12))
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(in)))
	if err != nil {
		return "", fmt.Errorf("private key is not valid base64: %w", err)
	}
	if len(raw) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("private key is %d bytes; want %d", len(raw), ed25519.PrivateKeySize)
	}
	priv := ed25519.PrivateKey(raw)
	if !bytes.Equal(ed25519.NewKeyFromSeed(priv.Seed()), priv) {
		return "", errors.New("private key is inconsistent: its public half does not match its seed")
	}
	return base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey)), nil
}
