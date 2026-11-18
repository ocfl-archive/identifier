// Package config defines the application configuration structures and TOML loader.
package config

import (
	"os"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/stashconfig"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

// Config holds the top-level configuration for the identifier tool,
// including indexer action settings and structured logging options (such as Logstash integration).
type Config struct {
	// Indexer configures the underlying technical metadata indexer actions (Siegfried, FFprobe, ImageMagick, Tika, etc.).
	Indexer *indexer.IndexerConfig `toml:"indexer"`

	// Log configures logging level, output file, and remote logstash/TLS settings.
	Log stashconfig.Config `toml:"log"`
}

// LoadConfig loads the application configuration.
// If configPath is empty, it returns the built-in default configuration embedded in minimal.toml.
// If configPath is provided, it loads the default configuration and overrides it with values from the TOML file.
func LoadConfig(configPath string) (*Config, error) {
	var conf = &Config{
		Indexer: indexer.GetDefaultConfig(),
		Log: stashconfig.Config{
			Level: "ERROR",
		},
	}
	if err := toml.Unmarshal(DefaultConfig, conf); err != nil {
		return nil, errors.Wrap(err, "cannot load default config")
	}
	if configPath == "" {
		return conf, nil
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "config file %s does not exist", configPath)
	}

	if _, err := toml.DecodeFile(configPath, conf); err != nil {
		return nil, errors.Wrapf(err, "Error unmarshalling config")
	}
	return conf, nil
}
