package commands

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"regexp"
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

var indexMimeCmd = &cobra.Command{
	Use:     "mime",
	Aliases: []string{},
	Short:   "get mime statistics from database",
	Long: `get mimestatistics from database
`,
	Example: ``,
	Args:    nil,
	Run:     doindexMime,
}

func indexMimeInit() {
	indexMimeCmd.Flags().StringVar(&dbFolderIndexMimeFlag, "database", "", "folder for database (must already exist)")
	indexMimeCmd.Flags().StringVar(&csvIndexMimeFlag, "csv", "", "write indexMime to csv file")
	indexMimeCmd.Flags().StringVar(&jsonlIndexMimeFlag, "jsonl", "", "write indexMime to jsonl file")
	indexMimeCmd.Flags().StringVar(&xlsxIndexMimeFlag, "xlsx", "", "write indexMime to xlsx file (needs memory)")
	indexMimeCmd.Flags().BoolVar(&emptyIndexMimeFlag, "empty", false, "include empty files")
	indexMimeCmd.Flags().StringVar(&regexpIndexMimeFlag, "regexp", "", "include files matching regular expression")
	indexMimeCmd.Flags().BoolVar(&duplicatesIndexMimeFlag, "duplicates", false, "include duplicate files")
	indexMimeCmd.Flags().StringVar(&prefixIndexMimeFlag, "prefix", "", "folder path prefix")
	indexMimeCmd.Flags().BoolVar(&consoleIndexMimeFlag, "console", false, "write index to console")
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
	if err := badgerIterator.IterateIndex(prefixIndexFolderFlag, func(fData *identifier.FileData) (remove bool, err error) {
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
