package pglite

import "os"

// Config holds configuration for a PGlite instance.
type Config struct {
	// DataDir is the host directory used for PostgreSQL data files.
	// Defaults to a temporary directory.
	DataDir string

	// Database is the PostgreSQL database name. Defaults to "postgres".
	Database string

	// User is the PostgreSQL user name. Defaults to "postgres".
	User string

	// StdoutFile is the path to a file for PostgreSQL stdout output.
	// If empty, stdout is discarded (/dev/null).
	StdoutFile string

	// StderrFile is the path to a file for PostgreSQL stderr output.
	// If empty, stderr is discarded (/dev/null).
	StderrFile string
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.Database == "" {
		out.Database = "postgres"
	}
	if out.User == "" {
		out.User = "postgres"
	}
	if out.StdoutFile == "" {
		out.StdoutFile = os.DevNull
	}
	if out.StderrFile == "" {
		out.StderrFile = os.DevNull
	}
	return out
}
