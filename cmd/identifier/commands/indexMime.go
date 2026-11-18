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
	var err error

	var regex *regexp.Regexp
	if regexpIndexMimeFlag != "" {
		regex, err = regexp.Compile(regexpIndexMimeFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", regexpIndexMimeFlag)
			defer os.Exit(1)
			return
		}
		fmt.Printf("#including regexp \"%s\"\n", regexpIndexMimeFlag)
	}
	if emptyIndexMimeFlag {
		fmt.Println("#including empty files")
	}
	if duplicatesIndexMimeFlag {
		fmt.Println("#including duplicate files")
	}
	if prefixIndexMimeFlag != "" {
		fmt.Printf("#including prefix \"%s\"\n", prefixIndexMimeFlag)
	}
	if !emptyIndexMimeFlag && !duplicatesIndexMimeFlag && regexpIndexMimeFlag == "" {
		fmt.Println("#including all files")
	}
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

	var statSize = map[string]int64{}
	var statCount = map[string]int64{}
	if err := badgerIterator.IterateIndex(prefixIndexMimeFlag, func(fData *identifier.FileData) (remove bool, err error) {
		if fData.Basename == "" || fData.Indexer == nil {
			return false, nil
		}
		var hit bool
		hit = (emptyIndexMimeFlag && fData.Size == 0) ||
			(duplicatesIndexMimeFlag && fData.Duplicate) ||
			(regex != nil && regex.MatchString(fData.Basename)) ||
			(!emptyIndexMimeFlag && !duplicatesIndexMimeFlag && regex == nil)
		if hit {
			if _, ok := statSize[fData.Indexer.Mimetype]; !ok {
				statSize[fData.Indexer.Mimetype] = 0
				statCount[fData.Indexer.Mimetype] = 0
			}
			statSize[fData.Indexer.Mimetype] += fData.Size
			statCount[fData.Indexer.Mimetype]++
		}
		return false, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	tw := table.NewWriter()
	header := table.Row{}
	for _, field := range fieldsIndexMime {
		header = append(header, field)
	}
	tw.AppendHeader(header)

	for mime, size := range statSize {
		if err := output.Write([]any{
			mime,
			statCount[mime],
			size,
			humanize.Bytes(uint64(size)),
		}, struct {
			Mime  string
			Size  int64
			Count int64
		}{Mime: mime, Size: size, Count: statCount[mime]}); err != nil {
			logger.Error().Err(err).Msg("cannot write output")
		}
		tw.AppendRow(table.Row{mime, statCount[mime], size, humanize.Bytes(uint64(size))})
	}
	tw.SetTitle("Mime statistics")
	if consoleIndexMimeFlag {
		fmt.Println(tw.Render())
	}

	return
}
