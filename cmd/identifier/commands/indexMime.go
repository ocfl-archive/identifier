package commands

import (
	"os"

	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var csvIndexMimeFlag string
var jsonlIndexMimeFlag string
var xlsxIndexMimeFlag string
var dbFolderIndexMimeFlag string
var emptyIndexMimeFlag bool
var regexpIndexMimeFlag string
var duplicatesIndexMimeFlag bool
var prefixIndexMimeFlag string
var consoleIndexMimeFlag bool

var fieldsIndexMime = []string{"mimetype", "count", "size (bytes)", "size"}

// indexMimeCmd computes aggregate file format distribution statistics grouped by MIME type
// from the indexed Badger database cache.
var indexMimeCmd = &cobra.Command{
	Use:     "mime",
	Aliases: []string{},
	Short:   "get mime statistics from database",
	Long: `Get MIME type statistics from database.
Aggregates file counts and total byte volume per MIME type (e.g. application/pdf, audio/x-wav, image/jpeg).`,
	Example: `Show MIME statistics in console table:
` + appname + ` index mime --database C:/temp/live --console

Export MIME statistics to CSV and Excel:
` + appname + ` index mime --database C:/temp/live --csv C:/temp/mime.csv --xlsx C:/temp/mime.xlsx`,
	Args: nil,
	Run:  doindexMime,
}

func indexMimeInit() {
	indexMimeCmd.Flags().StringVar(&dbFolderIndexMimeFlag, "database", "", "folder for Badger database storage (must already exist)")
	indexMimeCmd.Flags().StringVar(&csvIndexMimeFlag, "csv", "", "write MIME statistics to CSV file")
	indexMimeCmd.Flags().StringVar(&jsonlIndexMimeFlag, "jsonl", "", "write MIME statistics to JSONL file")
	indexMimeCmd.Flags().StringVar(&xlsxIndexMimeFlag, "xlsx", "", "write MIME statistics to Excel XLSX file")
	indexMimeCmd.Flags().BoolVar(&emptyIndexMimeFlag, "empty", false, "filter for empty files")
	indexMimeCmd.Flags().StringVar(&regexpIndexMimeFlag, "regexp", "", "filter files whose basename matches regular expression")
	indexMimeCmd.Flags().BoolVar(&duplicatesIndexMimeFlag, "duplicates", false, "filter for duplicate files")
	indexMimeCmd.Flags().StringVar(&prefixIndexMimeFlag, "prefix", "", "filter files by folder path prefix")
	indexMimeCmd.Flags().BoolVar(&consoleIndexMimeFlag, "console", false, "print rendered MIME statistics table to console")
	indexMimeCmd.MarkFlagRequired("database")
}

func doindexMime(cmd *cobra.Command, args []string) {
	regex := CompileRegexOrExit(regexpIndexMimeFlag)
	LogFilterSummary(emptyIndexMimeFlag, duplicatesIndexMimeFlag, regexpIndexMimeFlag, prefixIndexMimeFlag)

	output, err := identifier.NewOutput(consoleIndexMimeFlag || (csvIndexMimeFlag == "" && jsonlIndexMimeFlag == "" && xlsxIndexMimeFlag == ""), csvIndexMimeFlag, jsonlIndexMimeFlag, xlsxIndexMimeFlag, "list", fieldsIndexMime, logger)
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

	badgerIterator, err := identifier.NewBadgerIterator(dbFolderIndexMimeFlag, true, logger)
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

	filter := identifier.BuildFileFilter(emptyIndexMimeFlag, duplicatesIndexMimeFlag, regex)
	stats, err := AggregateFileData(badgerIterator, prefixIndexMimeFlag, filter, func(fData *identifier.FileData) string {
		return fData.Indexer.Mimetype
	})
	if err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}

	type jsonMime struct {
		Mime  string
		Size  int64
		Count int64
	}
	OutputStats(stats, "Mime statistics", fieldsIndexMime, output, consoleIndexMimeFlag, func(rec *StatRecord) jsonMime {
		return jsonMime{Mime: rec.Key, Size: rec.Size, Count: rec.Count}
	})
}
