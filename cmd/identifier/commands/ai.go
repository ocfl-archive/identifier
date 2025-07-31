package commands

import (
	"bufio"
	"bytes"
	"context"
	"emperror.dev/errors"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/kilib/pkg/gemini"
	"github.com/je4/kilib/pkg/ki"
	"github.com/je4/kilib/pkg/openai"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var aiCmd = &cobra.Command{
	Use:     "ai",
	Aliases: []string{},
	Short:   "retrieves AI descriptions",
	Long:    `retrieves AI descriptions`,
	Example: ``,
	Args:    cobra.NoArgs,
	Run:     doAi,
}

var dbFolderAIFlag string
var prefixAIFlag string
var csvAIFlag string
var jsonlAIFlag string
var xlsxAIFlag string
var consoleAIFlag bool
var modelAIFlag string
var apikeyAIFlag string
var aiQuery string
var aiAdditionalQuery string

func aiInit() {
	aiCmd.Flags().StringVar(&dbFolderAIFlag, "database", "", "folder for database (must already exist)")
	aiCmd.Flags().StringVar(&prefixAIFlag, "prefix", "", "folder path prefix")
	aiCmd.Flags().StringVar(&csvAIFlag, "csv", "", "write ai to csv file")
	aiCmd.Flags().StringVar(&jsonlAIFlag, "jsonl", "", "write ai to jsonl file")
	aiCmd.Flags().StringVar(&xlsxAIFlag, "xlsx", "", "write ai to xlsx file (needs memory)")
	aiCmd.Flags().BoolVar(&consoleAIFlag, "console", false, "write ai to console")
	aiCmd.Flags().StringVar(&modelAIFlag, "model", "google-gemini-2.0-pro-exp-02-05", "model for ai")
	aiCmd.Flags().StringVar(&apikeyAIFlag, "apikey", "%%GEMINI_API_KEY%%", "apikey for ai")
	aiCmd.Flags().StringVar(&aiQuery, "query", "", "query for ai")
	aiCmd.Flags().StringVar(&aiAdditionalQuery, "additional-query", "", "additional query for ai, will be prepended to the main query")
	aiCmd.MarkFlagDirname("database")
	aiCmd.MarkFlagRequired("database")
	aiCmd.MarkFlagFilename("jsonl", "jsonl", "json")
	aiCmd.MarkFlagFilename("csv", "csv")
	aiCmd.MarkFlagFilename("xlsx", "xlsx")

	aiRoCrateInit()
	aiCmd.AddCommand(aiRoCrateCmd)
}

var envRegexp = regexp.MustCompile(`%%([A-Z0-9_]+)%%`)

type aiResultStruct struct {
	Folder      string
	Title       string
	Description string
	Place       string
	Date        string
}

var regexpJSON = regexp.MustCompile(`(?s)^[^{\[]*([{\[].*[}\]])[^}\]]*$`)

var fieldsAI = []string{"folder", "title", "description", "place", "date"}

func doAi(cmd *cobra.Command, args []string) {
	if matches := envRegexp.FindStringSubmatch(apikeyAIFlag); len(matches) > 1 {
		apikeyAIFlag = os.Getenv(matches[1])
	}
	if aiQuery == "" {
		aiQuery = `Erstelle basierend auf der "CSV-Datei" Metadaten für jeden Folder und 
fülle die leeren Felder der "JSON-Datei" für alle dort angegebenen Folder aus. Stelle sicher, dass jeder Folder genau einmal auftaucht.
Falls Folder oder Dateinamen Semantik beinhalten, Nutze diese für Titel und Beschreibung. Sollten Folder oder Dateinamen Rückschlüsse auf Ort oder Datum zulassen,
fülle diese Felder entsprechend aus. Date bitte im Format YYYY-MM-DD oder YYYY. Achte darauf, dass die Metadaten in der JSON-Datei im korrekten Format vorliegen. 
Die Felder "place" und "date" sind optional, aber sollten ausgefüllt werden, wenn die Informationen verfügbar sind.
Die Metadaten sollten in englischer Sprache verfasst sein.
Sprache ist Englisch und der Duktus wissenschaftlich. Achte darauf, dass das JSON Format korrekt eingehalten wird.`
	} else if fi, err := os.Stat(aiQuery); err == nil && !fi.IsDir() {
		// read query from file
		data, err := os.ReadFile(aiQuery)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot read query from file '%s'", aiQuery)
			defer os.Exit(1)
			return
		}
		aiQuery = string(data)
	}
	if aiAdditionalQuery != "" {
		if fi, err := os.Stat(aiAdditionalQuery); err == nil && !fi.IsDir() {
			// read additional query from file
			data, err := os.ReadFile(aiAdditionalQuery)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot read additional query from file '%s'", aiAdditionalQuery)
				defer os.Exit(1)
				return
			}
			aiAdditionalQuery = string(data)
		}
		aiQuery = aiAdditionalQuery + "\n\n" + aiQuery
	}
	modelAIFlag = strings.ToLower(modelAIFlag)
	modelParts := strings.SplitN(modelAIFlag, "-", 2)
	if len(modelParts) != 2 {
		logger.Error().Msgf("model '%s' must consist of driver- and modelname", modelAIFlag)
		defer os.Exit(1)
		return
	}
	driverName := modelParts[0]
	if driverName == "google" {
		driverName = "gemini"
	}
	var driver ki.Interface
	var err error
	switch strings.ToLower(driverName) {
	case "gemini":
		driver, err = gemini.NewDriver(modelParts[1], apikeyAIFlag)
	case "openai":
		driver, err = openai.NewDriver(modelParts[1], apikeyAIFlag)
	default:
		err = errors.Errorf("unknown driver '%s'", modelParts[0])
	}
	if err != nil {
		logger.Error().Err(err).Msgf("cannot create driver for '%s'", modelAIFlag)
		defer os.Exit(1)
		return
	}

	var badgerDB *badger.DB

	output, err := identifier.NewOutput(consoleAIFlag || (csvAIFlag == "" && jsonlAIFlag == "" && xlsxAIFlag == ""), csvAIFlag, jsonlAIFlag, xlsxAIFlag, "ai", fieldsAI, logger)
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

	if badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderAIFlag).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
		logger.Error().Err(err).Msgf("cannot open badger database in '%s'", dbFolderFlag)
		defer os.Exit(1)
		return
	}
	defer badgerDB.Close()

	var prefix = "file:" + prefixAIFlag
	var result = []*aiResultStruct{}
	var bytesBuffer = bytes.NewBuffer(nil)
	var contextWriter = bufio.NewWriter(bytesBuffer)
	var csvWriter = csv.NewWriter(contextWriter)
	csvWriter.Write([]string{"folder", "filename", "mimetype", "pronom", "type", "subtype", "size (bytes)"})
	if err := badgerDB.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = true
		options.Prefix = []byte(prefix)
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(val []byte) error {
				fData := &identifier.FileData{}
				if err := json.Unmarshal(val, fData); err != nil {
					return errors.Wrapf(err, "cannot unmarshal file data from key '%s'", k)
				}
				if fData.Indexer == nil {
					return errors.Errorf("no indexer data for '%s'", fData.Path)
				}
				csvWriter.Write([]string{filepath.ToSlash(fData.Folder), fData.Basename, fData.Indexer.Mimetype, fData.Indexer.Pronom, fData.Indexer.Type, fData.Indexer.Subtype, fmt.Sprintf("%d", fData.Indexer.Size)})
				result = append(result, &aiResultStruct{
					Folder: filepath.ToSlash(fData.Folder),
				})
				return nil
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		logger.Error().Err(err).Msgf("cannot iterate over badger database with prefix '%s'", prefixAIFlag)
		defer os.Exit(1)
		return
	}
	csvWriter.Flush()
	contextWriter.Flush()
	logger.Info().Msgf("writing %d files to csv", len(result))
	resultBytes, err := json.Marshal(result)
	if err != nil {
		logger.Error().Err(err).Msg("cannot marshal result")
		defer os.Exit(1)
		return
	}
	logger.Info().Msgf("querying %s", modelAIFlag)
	aiResult, aiUsage, err := driver.QueryWithText(context.Background(), aiQuery, []string{
		"CSV-Datei (erste Zeile enthält die Spaltenüberschriften):\n" + bytesBuffer.String(),
		"JSON-Datei:\n" + string(resultBytes),
	})
	if err != nil {
		logger.Error().Err(err).Msg("cannot query ai")
		defer os.Exit(1)
		return
	}
	var res string
	if matches := regexpJSON.FindStringSubmatch(strings.Join(aiResult, "\n")); len(matches) > 1 {
		res = matches[1]
	}

	if err := json.Unmarshal([]byte(res), &result); err != nil {
		logger.Error().Err(err).Msgf("cannot unmarshal result:\n%s", strings.Join(aiResult, "\n +++ \n"))
		defer os.Exit(1)
		return
	}
	if err := badgerDB.Update(func(txn *badger.Txn) error {
		for _, r := range result {
			data, err := json.Marshal(r)
			if err != nil {
				return errors.Wrapf(err, "cannot marshal result for '%s'", r.Folder)
			}
			if err := txn.Set([]byte(fmt.Sprintf("ai:%s:%s", modelAIFlag, r.Folder)), data); err != nil {
				return errors.Wrapf(err, "cannot write result for '%s'", r.Folder)
			}
			output.Write([]any{r.Folder, r.Title, r.Description, r.Place, r.Date}, r)
		}
		return nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot write result to badger")
	}

	_ = aiUsage

	return
}
