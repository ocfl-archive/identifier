package config

import (
	"os"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/stashconfig"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

type Config struct {
	Indexer *indexer.IndexerConfig
	Log     stashconfig.Config `toml:"log"`
}

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
