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
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
)

func main() {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fmt.Fprintln(os.Stderr, "genkey:", err)
		os.Exit(1)
	}
	fmt.Printf("AEI_ED25519_PRIVATE_KEY=%s\n", base64.StdEncoding.EncodeToString(priv))
	fmt.Printf("AEI_ED25519_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(pub))
}
