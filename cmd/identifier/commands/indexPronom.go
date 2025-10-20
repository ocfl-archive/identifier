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

var indexPronomCmd = &cobra.Command{
	Use:     "pronom",
	Aliases: []string{},
	Short:   "get pronom statistics from database",
	Long: `get pronomstatistics from database
`,
	Example: ``,
	Args:    nil,
	Run:     doindexPronom,
}

func indexPronomInit() {
	indexPronomCmd.Flags().StringVar(&dbFolderIndexPronomFlag, "database", "", "folder for database (must already exist)")
	indexPronomCmd.Flags().StringVar(&csvIndexPronomFlag, "csv", "", "write indexPronom to csv file")
	indexPronomCmd.Flags().StringVar(&jsonlIndexPronomFlag, "jsonl", "", "write indexPronom to jsonl file")
	indexPronomCmd.Flags().StringVar(&xlsxIndexPronomFlag, "xlsx", "", "write indexPronom to xlsx file (needs memory)")
	indexPronomCmd.Flags().BoolVar(&emptyIndexPronomFlag, "empty", false, "include empty files")
	indexPronomCmd.Flags().StringVar(&regexpIndexPronomFlag, "regexp", "", "include files matching regular expression")
	indexPronomCmd.Flags().BoolVar(&duplicatesIndexPronomFlag, "duplicates", false, "include duplicate files")
	indexPronomCmd.Flags().StringVar(&prefixIndexPronomFlag, "prefix", "", "folder path prefix")
	indexPronomCmd.Flags().BoolVar(&consoleIndexPronomFlag, "console", false, "write index to console")
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
	if err := badgerIterator.IterateIndex(prefixIndexFolderFlag, func(fData *identifier.FileData) (remove bool, err error) {
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
