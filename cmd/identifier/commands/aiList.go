package commands

import (
	"fmt"
	"os"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var csvAiListFlag string
var jsonlAiListFlag string
var xlsxAiListFlag string
var dbFolderAiListFlag string
var prefixAiListFlag string
var consoleAiListFlag bool

var fieldsAiList = []string{"key", "folder", "title", "description", "place", "date", "tags", "persons", "institutions"}

// aiListCmd reads cached AI description records from BadgerDB and outputs them to CSV, JSONL, XLSX, or console.
var aiListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{},
	Short:   "get AI metadata from database",
	Long: `Get AI metadata from database.
Retrieves previously generated LLM descriptions (title, description, place, date, tags, persons, institutions)
from the BadgerDB storage without re-invoking the AI model APIs.`,
	Example: `List all AI descriptions from BadgerDB to console:
` + appname + ` ai list --database C:/temp/live --console

Export stored AI descriptions to CSV and Excel:
` + appname + ` ai list --database C:/temp/live --csv C:/temp/ai_export.csv --xlsx C:/temp/ai_export.xlsx`,
	Args: cobra.NoArgs,
	Run:  doaiList,
}

func aiListInit() {
	aiListCmd.Flags().StringVar(&dbFolderAiListFlag, "database", "", "folder for Badger database storage (must already exist)")
	aiListCmd.Flags().StringVar(&csvAiListFlag, "csv", "", "write AI metadata list to CSV file")
	aiListCmd.Flags().StringVar(&jsonlAiListFlag, "jsonl", "", "write AI metadata list to JSONL file")
	aiListCmd.Flags().StringVar(&xlsxAiListFlag, "xlsx", "", "write AI metadata list to Excel XLSX file")
	aiListCmd.Flags().StringVar(&prefixAiListFlag, "prefix", "", "filter records by folder path prefix")
	aiListCmd.Flags().BoolVar(&consoleAiListFlag, "console", false, "print AI metadata records to console")
	aiListCmd.MarkFlagRequired("database")
}

func doaiList(cmd *cobra.Command, args []string) {
	var dataPath string
	var err error
	if len(args) > 0 {
		dataPath, err = identifier.Fullpath(args[0])
		cobra.CheckErr(err)
		if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
			cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
		}
	}

	if prefixAiListFlag != "" {
		fmt.Printf("#including prefix \"%s\"\n", prefixAiListFlag)
	}
	output, err := identifier.NewOutput(
		consoleAiListFlag || (csvAiListFlag == "" && jsonlAiListFlag == "" && xlsxAiListFlag == ""),
		csvAiListFlag,
		jsonlAiListFlag,
		xlsxAiListFlag,
		"list",
		fieldsAiList,
		logger,
	)
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

	badgerIterator, err := identifier.NewBadgerIterator(dbFolderAiListFlag, true, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create badger reader")
		defer os.Exit(1)
		return
	}
	defer func() {
		if err := badgerIterator.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close badger reader")
		}
	}()

	if err := badgerIterator.IterateAI("ai:"+prefixAiListFlag, func(key string, aiData *identifier.AIResultStruct) (remove bool, err error) {
		var persons []string
		for _, person := range aiData.Persons {
			persons = append(persons, person.String())
		}
		if err := output.Write([]any{
			key,
			aiData.Folder,
			aiData.Title,
			aiData.Description,
			aiData.Place,
			aiData.Date,
			strings.Join(aiData.Tags, "; "),
			strings.Join(persons, "; "),
			strings.Join(aiData.Institutions, "; "),
		},
			aiData); err != nil {
			return false, errors.Wrapf(err, "cannot write output")
		}
		return false, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	return
}
