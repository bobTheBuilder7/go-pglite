package pglite

import _ "embed"

//go:embed pglite-wasi-17.tar.gz
var embeddedWASI []byte

func init() {
	WASIBinary = embeddedWASI
}
