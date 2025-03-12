package commands

import (
	"crypto/tls"
	"emperror.dev/errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/je4/identifier/config"
	"github.com/je4/identifier/identifier"
	"github.com/je4/identifier/version"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/cobra"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"io"
	"os"
)

var appname = "identifier"

var rootCmd = &cobra.Command{
	Use:   appname,
	Short: "identifier is a tool for technical metadata identification",
	Long: `A tool for technical metadata identification bases on indexer.
source code is available at: https://github.com/je4/identifier
by Jürgen Enge (University Library Basel, juergen@info-age.net)`,
	Version: fmt.Sprintf("%s '%s' (%s)", version.Version, version.ShortCommit(), version.Date),
	Run: func(cmd *cobra.Command, args []string) {
		if showConfig {
			toml.NewEncoder(os.Stdout).Encode(conf)
		} else {
			_ = cmd.Help()
		}
	},
}

// all possible flags of all modules go here
var persistentFlagConfigFile string
var persistentFlagLogfile string
var persistentFlagLoglevel string

var conf *config.Config
var logger zLogger.ZLogger

var showConfig bool

func rootInit() {
	indexCmd.Flags().BoolVar(&showConfig, "show-config", false, "show configuration")
}
func initConfig() {
	var data = []byte{}
	// load config file
	if persistentFlagConfigFile != "" {
		var err error
		persistentFlagConfigFile, err = identifier.Fullpath(persistentFlagConfigFile)
		if err != nil {
			cobra.CheckErr(errors.Errorf("cannot convert '%s' to absolute path: %v", persistentFlagConfigFile, err))
			return
		}
		log.Info().Msgf("loading configuration from %s", persistentFlagConfigFile)
		data, err = os.ReadFile(persistentFlagConfigFile)
		if err != nil {
			_ = rootCmd.Help()
			log.Error().Msgf("error reading config file %s: %v\n", persistentFlagConfigFile, err)
			os.Exit(1)
		}
	} else {
		data = config.DefaultConfig
	}
	var err error
	conf, err = config.LoadConfig(data)
	if err != nil {
		_ = rootCmd.Help()
		log.Error().Err(err).Msgf("cannot load config '%s'", persistentFlagConfigFile)
		os.Exit(1)
	}

	if persistentFlagLogfile != "" {
		conf.Log.File = persistentFlagLogfile
	}
	if persistentFlagLoglevel != "" {
		conf.Log.Level = persistentFlagLoglevel
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatal().Err(err).Msg("cannot create client loader")
		}
		defer loggerLoader.Close()
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		log.Fatal().Msg("cannot create logger")
	}
	if _logstash != nil {
		defer _logstash.Close()
	}

	if _logfile != nil {
		defer _logfile.Close()
	}

	l2 := _logger.With().Timestamp().Logger() //.Output(output)
	logger = &l2

	return
}

func init() {

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&persistentFlagConfigFile, "config", "", "config file (default is embedded)")
	rootCmd.PersistentFlags().StringVar(&persistentFlagLogfile, "log-file", "", "log output file (default is console)")
	rootCmd.PersistentFlags().StringVar(&persistentFlagLoglevel, "log-level", "", "log level (CRITICAL|ERROR|WARNING|NOTICE|INFO|DEBUG)")

	rootInit()
	clearpathInit()
	removeFilesInit()
	removeFoldersInit()
	indexInit()
	rootCmd.AddCommand(clearpathCmd, removeFilesCmd, removeFoldersCmd, indexCmd)
}
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
