package commands

import (
	"os"

	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var csvIndexPronomFlag string
var jsonlIndexPronomFlag string
var xlsxIndexPronomFlag string
var dbFolderIndexPronomFlag string
var emptyIndexPronomFlag bool
var regexpIndexPronomFlag string
var duplicatesIndexPronomFlag bool
var prefixIndexPronomFlag string
var consoleIndexPronomFlag bool

var fieldsIndexPronom = []string{"pronom", "count", "size (bytes)", "size"}

// indexPronomCmd computes aggregate file format distribution statistics grouped by PRONOM identifier
// (e.g. fmt/141, fmt/18, fmt/276) from the indexed Badger database cache.
var indexPronomCmd = &cobra.Command{
	Use:     "pronom",
	Aliases: []string{},
	Short:   "get pronom statistics from database",
	Long: `Get PRONOM statistics from database.
Aggregates file counts and total byte volume per National Archives PRONOM format registry ID (e.g. fmt/18, fmt/276, x-fmt/421).`,
	Example: `Show PRONOM statistics in console table:
` + appname + ` index pronom --database C:/temp/live --console

Export PRONOM statistics to CSV and Excel:
` + appname + ` index pronom --database C:/temp/live --csv C:/temp/pronom.csv --xlsx C:/temp/pronom.xlsx`,
	Args: nil,
	Run:  doindexPronom,
}

func indexPronomInit() {
	indexPronomCmd.Flags().StringVar(&dbFolderIndexPronomFlag, "database", "", "folder for Badger database storage (must already exist)")
	indexPronomCmd.Flags().StringVar(&csvIndexPronomFlag, "csv", "", "write PRONOM statistics to CSV file")
	indexPronomCmd.Flags().StringVar(&jsonlIndexPronomFlag, "jsonl", "", "write PRONOM statistics to JSONL file")
	indexPronomCmd.Flags().StringVar(&xlsxIndexPronomFlag, "xlsx", "", "write PRONOM statistics to Excel XLSX file")
	indexPronomCmd.Flags().BoolVar(&emptyIndexPronomFlag, "empty", false, "filter for empty files")
	indexPronomCmd.Flags().StringVar(&regexpIndexPronomFlag, "regexp", "", "filter files whose basename matches regular expression")
	indexPronomCmd.Flags().BoolVar(&duplicatesIndexPronomFlag, "duplicates", false, "filter for duplicate files")
	indexPronomCmd.Flags().StringVar(&prefixIndexPronomFlag, "prefix", "", "filter files by folder path prefix")
	indexPronomCmd.Flags().BoolVar(&consoleIndexPronomFlag, "console", false, "print rendered PRONOM statistics table to console")
	indexPronomCmd.MarkFlagRequired("database")
}

func doindexPronom(cmd *cobra.Command, args []string) {
	regex := CompileRegexOrExit(regexpIndexPronomFlag)
	LogFilterSummary(emptyIndexPronomFlag, duplicatesIndexPronomFlag, regexpIndexPronomFlag, prefixIndexPronomFlag)

	output, err := identifier.NewOutput(consoleIndexPronomFlag || (csvIndexPronomFlag == "" && jsonlIndexPronomFlag == "" && xlsxIndexPronomFlag == ""), csvIndexPronomFlag, jsonlIndexPronomFlag, xlsxIndexPronomFlag, "list", fieldsIndexPronom, logger)
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

	badgerIterator, err := identifier.NewBadgerIterator(dbFolderIndexPronomFlag, true, logger)
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

	filter := identifier.BuildFileFilter(emptyIndexPronomFlag, duplicatesIndexPronomFlag, regex)
	stats, err := AggregateFileData(badgerIterator, prefixIndexPronomFlag, filter, func(fData *identifier.FileData) string {
		return fData.Indexer.Pronom
	})
	if err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}

	type jsonPronom struct {
		Pronom string
		Size   int64
		Count  int64
	}
	OutputStats(stats, "Pronom statistics", fieldsIndexPronom, output, consoleIndexPronomFlag, func(rec *StatRecord) jsonPronom {
		return jsonPronom{Pronom: rec.Key, Size: rec.Size, Count: rec.Count}
	})
}
