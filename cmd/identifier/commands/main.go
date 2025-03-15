//go:build exclude

package commands

import (
	"crypto/tls"
	_ "embed"
	"emperror.dev/errors"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/identifier/identifier"
	indexerutil "github.com/ocfl-archive/indexer/v3/pkg/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

//go:embed ../../../config/minimal.toml
var configToml []byte

var configFlag = flag.String("config", "", "config file")
var folder = flag.String("path", "", "path to iterate")
var jsonFlag = flag.String("json", "", "json file to write")
var csvFlag = flag.String("csv", "", "csv file to write")
var badgerFlag = flag.String("db", "", "badger db folder to use")
var concurrentFlag = flag.Uint("n", 3, "number of concurrent workers")
var actionsFlag = flag.String("actions", "", "comma separated actions to perform")
var emptyFlag = flag.Bool("empty", false, "show empty files")
var duplicateFlag = flag.Bool("duplicate", false, "show duplicate files")
var baseNameFlag = flag.String("basename", "", "go regexp of basename to search for")
var dirNameFlag = flag.String("dirname", "", "go regexp of dirname to search for")
var removeFlag = flag.Bool("remove", false, "remove all found files")
var clearPathFlag = flag.Bool("clear", false, "clear path")
var renameFlag = flag.Bool("rename", false, "rename files")

func main() {
	flag.Parse()

	// create logger instance

	cfgToml := configToml
	if *configFlag != "" {
		data, err := os.ReadFile(*configFlag)
		if err != nil {
			log.Fatalf("cannot read config file: %v", err)
		}
		cfgToml = data
	}
	conf, err := indexerutil.LoadConfig(cfgToml)
	if err != nil {
		panic(fmt.Errorf("cannot load config: %v", err))
	}

	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var jsonlOutfile io.WriteCloser
	if *jsonFlag != "" {
		jsonlOutfile, err = os.Create(*jsonFlag)
		if err != nil {
			log.Fatalf("cannot create json file: %v", err)
		}
		defer jsonlOutfile.Close()
	}
	var csvOutfile io.WriteCloser
	var csvWriter *csv.Writer
	var badgerDB *badger.DB
	var baseNameRegexp *regexp.Regexp
	var dirNameRegexp *regexp.Regexp
	if *badgerFlag != "" {
		fi, err := os.Stat(*badgerFlag)
		if err != nil {
			log.Fatalf("cannot stat badger db folder: %v", err)
		}
		if !fi.IsDir() {
			log.Fatalf("badger db folder is not a directory: %v", err)
		}
		badgerDB, err = badger.Open(badger.DefaultOptions(*badgerFlag).WithCompression(badgerOptions.Snappy))
		if err != nil {
			log.Fatalf("cannot open badger db: %v", err)
		}
		defer badgerDB.Close()
	}
	if *csvFlag != "" {
		csvOutfile, err = os.Create(*csvFlag)
		if err != nil {
			log.Fatalf("cannot create csv file: %v", err)
		}
		defer csvOutfile.Close()

		csvWriter = csv.NewWriter(csvOutfile)
		defer csvWriter.Flush()
		csvWriter.Write([]string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"})
	}
	if *baseNameFlag != "" {
		baseNameRegexp, err = regexp.Compile(*baseNameFlag)
		if err != nil {
			log.Fatalf("cannot compile basename regexp '%s': %v", *baseNameFlag, err)
		}
	}

	if *dirNameFlag != "" {
		dirNameRegexp, err = regexp.Compile(*dirNameFlag)
		if err != nil {
			log.Fatalf("cannot compile dirname regexp '%s': %v", *dirNameFlag, err)
		}
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
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
		log.Fatalf("cannot create logger: %v", err)
	}
	if _logstash != nil {
		defer _logstash.Close()
	}

	if _logfile != nil {
		defer _logfile.Close()
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	if *clearPathFlag || baseNameRegexp != nil || dirNameRegexp != nil {
		if *emptyFlag || *duplicateFlag {
			logger.Fatal().Msg("cannot use -clear with -empty or -duplicate")
			return
		}
		if *folder == "" {
			logger.Fatal().Msg("need -path to clear path")
			return
		}
		if strings.HasPrefix(*folder, "./") {
			currDir, err := os.Getwd()
			if err != nil {
				logger.Fatal().Err(err).Msg("cannot get working directory")
			}
			*folder = filepath.Join(currDir, *folder)
		}
		dirFS := os.DirFS(*folder)
		pathElements, err := identifier.BuildPath(dirFS)
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot build path")
			return
		}
		if *clearPathFlag {
			for name, newName := range pathElements.ClearIterator {
				fmt.Printf("    %s\n--> %s\n\n", name, newName)
				if *renameFlag {
					fullpath := filepath.Join(*folder, name)
					newpath := filepath.Join(*folder, newName)
					logger.Info().Msgf("renaming '%s' to '%s'", fullpath, newpath)
					if err := os.Rename(fullpath, newpath); err != nil {
						logger.Fatal().Err(err).Msgf("cannot rename '%s' to '%s'", fullpath, newpath)
						return
					}
				}
			}
			return
		}
		if baseNameRegexp != nil {
			for name := range pathElements.FindBasename(baseNameRegexp) {
				fmt.Printf("%s\n", name)
				if *removeFlag {
					fullpath := filepath.Join(*folder, name)
					logger.Info().Msgf("removing %s", fullpath)
					if err := os.Remove(fullpath); err != nil {
						logger.Fatal().Err(err).Msgf("cannot remove %s", fullpath)
						return
					}
				}
			}
			return
		}
		if dirNameRegexp != nil {
			for name := range pathElements.FindDirname(dirNameRegexp) {
				fmt.Printf("%s\n", name)
				if *removeFlag {
					fullpath := filepath.Join(*folder, name)
					logger.Info().Msgf("removing %s", fullpath)
					if err := os.RemoveAll(fullpath); err != nil {
						logger.Fatal().Err(err).Msgf("cannot remove %s", fullpath)
						return
					}
				}
			}
			return
		}
		return
	}
	if *emptyFlag || *duplicateFlag {
		if *clearPathFlag {
			logger.Fatal().Msg("cannot use -empty or -duplicate or -basename with -clear")
			return
		}
		var dirFS fs.FS
		if *removeFlag && *folder == "" {
			logger.Fatal().Msg("need -path to remove files")
			return
		}
		if *folder != "" && *removeFlag {
			if strings.HasPrefix(*folder, "./") {
				currDir, err := os.Getwd()
				if err != nil {
					panic(fmt.Errorf("cannot get working directory: %v", err))
				}
				*folder = filepath.Join(currDir, *folder)
			}
			fi, err := os.Stat(*folder)
			if err != nil {
				logger.Fatal().Err(err).Msgf("cannot stat folder %s", *folder)
				return
			}
			if !fi.IsDir() {
				logger.Fatal().Msgf("%s is not a directory", *folder)
				return
			}
			dirFS = os.DirFS(*folder)
		}

		if badgerDB == nil {
			logger.Fatal().Msg("need badger db to show empty files (-db)")
			return
		}
		removedKeys := [][]byte{}
		if err := badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				if !strings.HasPrefix(string(k), "file:") {
					continue
				}
				err := item.Value(func(v []byte) error {
					fData := &identifier.FileData{}
					if err := json.Unmarshal(v, fData); err != nil {
						return errors.Wrapf(err, "cannot unmarshal value")
					}
					var baseNameFit bool
					if baseNameRegexp != nil {
						baseNameFit = baseNameRegexp.MatchString(fData.Basename)
					}
					if (*emptyFlag && fData.Size == 0) || (*duplicateFlag && fData.Duplicate) || (baseNameFit) {
						if csvWriter != nil {
							identifier.CsvWriteLine(csvWriter, fData)
						}
						if jsonlOutfile != nil {
							identifier.JsonlWriteLine(jsonlOutfile, fData)
						}
						if *removeFlag && dirFS != nil {
							fullpath := filepath.Join(*folder, fData.Path)
							logger.Info().Msgf("removing %s", fullpath)
							if err := os.Remove(fullpath); err != nil {
								logger.Error().Err(err).Str("folder", fData.Folder).Str("basename", fData.Basename).Msg("cannot remove file")
							} else {
								removedKeys = append(removedKeys, k)
							}
						}
					}
					identifier.WriteConsole(fData, 0, "./", true)
					logger.Info().Str("key", string(k)).Msg("key")
					return nil
				})
				if err != nil {
					return errors.Wrapf(err, "cannot read value")
				}
			}
			return nil
		}); err != nil {
			logger.Error().Err(err).Msg("cannot read badger db")
		}
		if len(removedKeys) > 0 {
			if err := badgerDB.Update(func(txn *badger.Txn) error {
				for _, k := range removedKeys {
					if err := txn.Delete(k); err != nil {
						return errors.Wrapf(err, "cannot delete key")
					}
				}
				return nil
			}); err != nil {
				logger.Error().Err(err).Msg("cannot delete keys")
			}
		}
		return
	}

	idx, err := util.InitIndexer(conf.Indexer, logger)
	if err != nil {
		panic(fmt.Errorf("cannot init indexer: %v", err))
	}

	if *folder == "" {
		*folder = "./"
	}
	if strings.HasPrefix(*folder, "./") {
		currDir, err := os.Getwd()
		if err != nil {
			panic(fmt.Errorf("cannot get working directory: %v", err))
		}
		*folder = filepath.Join(currDir, *folder)
	}
	dirFS := os.DirFS(*folder)
	//zipFS, err := zipasfolder.NewFS(dirFS, 10, true, logger)
	zipFS := dirFS
	/*
		if err != nil {
			panic(fmt.Errorf("cannot create zip as folder FS: %v", err))
		}
	*/

	jobs := make(chan string, 100)
	results := make(chan string, 100)

	startTime := time.Now().Unix()
	for w := uint(1); w <= *concurrentFlag; w++ {
		go identifier.Worker(w, zipFS, idx, logger, jobs, results, jsonlOutfile, csvWriter, badgerDB, startTime)
	}

	go func() {
		for n := range results {
			fmt.Printf("%s\n", n)
		}
	}()

	if err := fs.WalkDir(zipFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return errors.Wrapf(err, "cannot walk %s/%s", dirFS, path)
		}
		if d.IsDir() {
			fmt.Printf("[d] %s/%s\n", zipFS, path)
			return nil
		}
		fmt.Printf("[f] %s/%s\n", zipFS, path)
		isZip := strings.Contains(path, ".zip")
		if !isZip {
			//			return nil
		}

		identifier.Waiter.Add(1)
		jobs <- path

		return nil
	}); err != nil {
		panic(fmt.Errorf("cannot walkd folder %v: %v", dirFS, err))
	}

	identifier.Waiter.Wait()
	close(jobs)
}
