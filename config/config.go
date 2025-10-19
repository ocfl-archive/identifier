package config

import (
	"os"
	"os/user"
	"path/filepath"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/stashconfig"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

type Config struct {
	Indexer *indexer.IndexerConfig
	Log     stashconfig.Config `toml:"log"`
}

func LoadConfig(tomlBytes []byte) (*Config, error) {
	var conf = &Config{
		Indexer: &indexer.IndexerConfig{},
		Log: stashconfig.Config{
			Level: "ERROR",
		},
	}

	if err := toml.Unmarshal(tomlBytes, conf); err != nil {
		return nil, errors.Wrapf(err, "Error unmarshalling config")
	}
	if conf.Indexer.Siegfried.SignatureFile == "" {
		user, err := user.Current()
		if err != nil {
			return nil, errors.Wrap(err, "cannot get current user")
		}
		fp := filepath.Join(user.HomeDir, "siegfried", "default.sig")
		fi, err := os.Stat(fp)
		if err == nil && !fi.IsDir() {
			conf.Indexer.Siegfried.SignatureFile = fp
		}
	}
	return conf, nil
}
