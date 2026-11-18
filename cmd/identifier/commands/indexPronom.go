package commands

import (
	"fmt"
	"os"
	"regexp"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
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
	var err error

	var regex *regexp.Regexp
	if regexpIndexPronomFlag != "" {
		regex, err = regexp.Compile(regexpIndexPronomFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", regexpIndexPronomFlag)
			defer os.Exit(1)
			return
		}
		fmt.Printf("#including regexp \"%s\"\n", regexpIndexPronomFlag)
	}
	if emptyIndexPronomFlag {
		fmt.Println("#including empty files")
	}
	if duplicatesIndexPronomFlag {
		fmt.Println("#including duplicate files")
	}
	if prefixIndexPronomFlag != "" {
		fmt.Printf("#including prefix \"%s\"\n", prefixIndexPronomFlag)
	}
	if !emptyIndexPronomFlag && !duplicatesIndexPronomFlag && regexpIndexPronomFlag == "" {
		fmt.Println("#including all files")
	}
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

	var statSize = map[string]int64{}
	var statCount = map[string]int64{}
	if err := badgerIterator.IterateIndex(prefixIndexPronomFlag, func(fData *identifier.FileData) (remove bool, err error) {
		if fData.Basename == "" || fData.Indexer == nil {
			return false, nil
		}

		var hit bool
		hit = (emptyIndexPronomFlag && fData.Size == 0) ||
			(duplicatesIndexPronomFlag && fData.Duplicate) ||
			(regex != nil && regex.MatchString(fData.Basename)) ||
			(!emptyIndexPronomFlag && !duplicatesIndexPronomFlag && regex == nil)
		if hit {
			if _, ok := statSize[fData.Indexer.Pronom]; !ok {
				statSize[fData.Indexer.Pronom] = 0
				statCount[fData.Indexer.Pronom] = 0
			}
			statSize[fData.Indexer.Pronom] += fData.Size
			statCount[fData.Indexer.Pronom]++
		}
		return false, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	tw := table.NewWriter()
	header := table.Row{}
	for _, field := range fieldsIndexPronom {
		header = append(header, field)
	}
	tw.AppendHeader(header)

	for pronom, size := range statSize {
		if err := output.Write([]any{
			pronom,
			statCount[pronom],
			size,
			humanize.Bytes(uint64(size)),
		}, struct {
			Pronom string
			Size   int64
			Count  int64
		}{Pronom: pronom, Size: size, Count: statCount[pronom]}); err != nil {
			logger.Error().Err(err).Msg("cannot write output")
		}
		tw.AppendRow(table.Row{pronom, statCount[pronom], size, humanize.Bytes(uint64(size))})
	}
	tw.SetTitle("Pronom statistics")
	if consoleIndexPronomFlag {
		fmt.Println(tw.Render())
	}

	return
}
