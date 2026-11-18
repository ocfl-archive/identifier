package config

import _ "embed"

// DefaultConfig contains the embedded minimal.toml default configuration file.
//
//go:embed minimal.toml
var DefaultConfig []byte
