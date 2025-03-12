package config

import _ "embed"

//go:embed minimal.toml
var DefaultConfig []byte
