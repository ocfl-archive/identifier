package commands

import (
	"emperror.dev/errors"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"os"
	"path/filepath"
	"strings"
)

const defaultRoCrate = `{
	"@context": "https://w3id.org/ro/crate/1.1/context",
	"@graph": [
		{
		  "@type": "CreativeWork",
		  "@id": "ro-crate-metadata.json",
		  "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
		  "about": {"@id": "./"}
		},  
		{
		  "@id": "./",
		  "@type": [
			"Dataset"
		  ],
		  "hasPart": []
		}
	]
}`

var aiRoCrateCmd = &cobra.Command{
	Use:     "ro-crate [path to data]",
	Aliases: []string{},
	Short:   "writes AI Descriptions to Ro-Crate",
	Long:    `writes AI Descriptions to Ro-Crate`,
	Example: ``,
	Args:    cobra.ExactArgs(1),
	Run:     doAIRoCrate,
}

var dbFolderAIRoCrateFlag string
var prefixAIRoCrateFlag string
var modelAIRoCrateFlag string

func aiRoCrateInit() {
	aiRoCrateCmd.Flags().StringVar(&dbFolderAIRoCrateFlag, "database", "", "folder for database (must already exist)")
	aiRoCrateCmd.Flags().StringVar(&prefixAIRoCrateFlag, "prefix", "", "folder path prefix")
	aiRoCrateCmd.Flags().StringVar(&modelAIRoCrateFlag, "model", "google-gemini-2.0-pro-exp-02-05", "model for aiRoCrate")
	aiRoCrateCmd.MarkFlagDirname("database")
	aiRoCrateCmd.MarkFlagRequired("database")
	aiRoCrateCmd.MarkFlagDirname("prefix")
}

var fieldsAIRoCrate = []string{"folder", "title", "description"}

func doAIRoCrate(cmd *cobra.Command, args []string) {
	modelAIRoCrateFlag = strings.ToLower(modelAIRoCrateFlag)
	var dataPath string
	var err error
	if len(args) > 0 {
		dataPath, err = identifier.Fullpath(args[0])
		cobra.CheckErr(err)
		if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
			cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
		}
	}

	var badgerDB *badger.DB

	output, err := identifier.NewOutput(consoleIndexListFlag || (csvIndexListFlag == "" && jsonlIndexListFlag == "" && xlsxIndexListFlag == ""), csvIndexListFlag, jsonlIndexListFlag, xlsxIndexListFlag, "aiRoCrate", fieldsAIRoCrate, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create output")
		defer os.Exit(1)
		return
	}
	defer func() {
		if err := output.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close output")
		}
	}()

	if badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderAIRoCrateFlag).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
		logger.Error().Err(err).Msgf("cannot open badger database in '%s'", dbFolderFlag)
		defer os.Exit(1)
		return
	}
	defer badgerDB.Close()

	roCratePath := filepath.Join(dataPath, prefixAIRoCrateFlag, "ro-crate-metadata.json")
	fi, err := os.Stat(roCratePath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.WriteFile(roCratePath, []byte(defaultRoCrate), 0644); err != nil {
				logger.Error().Err(err).Msgf("cannot create '%s'", roCratePath)
				defer os.Exit(1)
				return
			}
		} else {
			logger.Error().Err(err).Msgf("cannot stat '%s'", roCratePath)
			defer os.Exit(1)
			return
		}
	}
	if fi.IsDir() {
		logger.Error().Msgf("'%s' is a directory", roCratePath)
		defer os.Exit(1)
		return
	}

	var roCrate = &identifier.RoCrate{}
	fp, err := os.Open(roCratePath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot open '%s'", roCratePath)
		defer os.Exit(1)
		return
	}
	if err := json.NewDecoder(fp).Decode(roCrate); err != nil {
		fp.Close()
		logger.Error().Err(err).Msgf("cannot decode '%s'", roCratePath)
		defer os.Exit(1)
		return
	}
	fp.Close()
	var prefix = fmt.Sprintf("ai:%s:%s", modelAIRoCrateFlag, prefixAIRoCrateFlag)
	//var result = []*aiResultStruct{}
	if err := badgerDB.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = true
		options.Prefix = []byte(prefix)
		it := txn.NewIterator(options)
		defer it.Close()
		var folderList = map[string]*identifier.RoCrateGraphElement{}
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(val []byte) error {
				data := &aiResultStruct{}
				if err := json.Unmarshal(val, data); err != nil {
					return errors.Wrapf(err, "cannot unmarshal file data from key '%s'", k)
				}
				logger.Info().Msgf("processing %s", data.Folder)
				id := strings.TrimSuffix(data.Folder, "/") + "/"
				folderList[id] = &identifier.RoCrateGraphElement{
					ID:          id,
					Type:        identifier.StringOrList{"Dataset"},
					Name:        data.Title,
					Description: data.Description,
				}
				return nil
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		var ids = []string{}
		for id, _ := range folderList {
			ids = append(ids, id)
		}

		slices.SortFunc(ids, func(a, b string) int {
			v := len(a) - len(b)
			switch {
			case v < 0:
				return -1
			case v > 0:
				return 1
			default:
				return 0
			}
		})
		for _, id := range ids {
			data := folderList[id]
			lastInd := strings.LastIndex(strings.TrimSuffix(id, "/"), "/")
			if lastInd <= 0 {
				return nil
			}
			parentID := id[:lastInd] + "/"
			parentElem := roCrate.Get(parentID)
			if parentElem == nil {
				roCrate.AddElement(data, false)
			} else {
				parentElem.AddChild(data, false)
			}
		}
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(val []byte) error {
				data := &aiResultStruct{}
				if err := json.Unmarshal(val, data); err != nil {
					return errors.Wrapf(err, "cannot unmarshal file data from key '%s'", k)
				}

				id := strings.TrimSuffix(data.Folder, "/")
				lastInd := strings.LastIndex(id, "/")
				if lastInd <= 0 {
					return nil
				}
				parentID := id[:lastInd] + "/"
				id += "/"

				elem := roCrate.Get(parentID)
				if elem == nil {
					logger.Error().Msgf("cannot find element '%s' in roCrate", id)
					return nil
				}
				return nil
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		logger.Error().Err(err).Msgf("cannot iterate over badger database with prefix '%s'", prefixAIRoCrateFlag)
		defer os.Exit(1)
		return
	}
	fp, err = os.Create(roCratePath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot create '%s'", roCratePath)
		defer os.Exit(1)
		return
	}
	defer fp.Close()
	jsonEnc := json.NewEncoder(fp)
	jsonEnc.SetIndent("", "  ")
	if err := jsonEnc.Encode(roCrate); err != nil {
		logger.Error().Err(err).Msgf("cannot encode '%s'", roCratePath)
		defer os.Exit(1)
		return
	}
	return
}
