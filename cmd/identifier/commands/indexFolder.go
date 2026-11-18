package commands

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	human "github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var csvIndexFolderFlag string
var jsonlIndexFolderFlag string
var xlsxIndexFolderFlag string
var consoleIndexFolderFlag bool
var prefixIndexFolderFlag string
var dbIndexFolderFlag string

// folderFields defines standard column headers for directory statistics.
var folderFields = []string{"Files", "Folders", "Bytes", "Size", "Path"}

// indexFoldersCmd calculates and displays aggregated folder statistics (file count, subfolder count, byte size)
// from the indexed Badger database cache without scanning the filesystem.
var indexFoldersCmd = &cobra.Command{
	Use:     "folders",
	Aliases: []string{},
	Short:   "get folder statistics from database",
	Long: `Get folder statistics from database.
Aggregates file counts, subfolder counts, and total byte sizes hierarchically from BadgerDB.`,
	Example: `Show folder statistics in console:
` + appname + ` index folders --database C:/temp/live --console

Export folder statistics to CSV and Excel:
` + appname + ` index folders --database C:/temp/live --csv C:/temp/folders.csv --xlsx C:/temp/folders.xlsx`,
	Args: cobra.NoArgs,
	Run:  doindexFolders,
}

func indexFoldersInit() {
	indexFoldersCmd.Flags().StringVar(&csvIndexFolderFlag, "csv", "", "write folder statistics to csv file")
	indexFoldersCmd.Flags().StringVar(&jsonlIndexFolderFlag, "jsonl", "", "write folder statistics to jsonl file")
	indexFoldersCmd.Flags().StringVar(&xlsxIndexFolderFlag, "xlsx", "", "write folder statistics to xlsx file (needs memory)")
	indexFoldersCmd.Flags().BoolVar(&consoleIndexFolderFlag, "console", false, "write folder statistics to console")
	indexFoldersCmd.Flags().StringVar(&prefixIndexFolderFlag, "prefix", "", "folder path prefix")
	indexFoldersCmd.Flags().StringVar(&dbIndexFolderFlag, "database", "", "folder for database (must already exist)")
	indexFoldersCmd.MarkFlagDirname("database")
	indexFoldersCmd.MarkFlagRequired("database")
	indexFoldersCmd.MarkFlagFilename("jsonl", "jsonl", "json")
	indexFoldersCmd.MarkFlagFilename("csv", "csv")
	indexFoldersCmd.MarkFlagFilename("xlsx", "xlsx")
}

func doindexFolders(cmd *cobra.Command, args []string) {
	output, err := identifier.NewOutput(false, csvIndexFolderFlag, jsonlIndexFolderFlag, xlsxIndexFolderFlag, "folders", folderFields, logger)
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

	badgerIterator, err := identifier.NewBadgerIterator(dbIndexFolderFlag, true, logger)
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

	var folders = identifier.NewPathElement("", true, 0, nil)
	if err := badgerIterator.IterateIndex(prefixIndexFolderFlag, func(fData *identifier.FileData) (remove bool, err error) {
		if fData.Basename == "" || fData.Indexer == nil {
			return false, nil
		}
		pathStr := path.Clean(filepath.ToSlash(fData.Path))
		pathParts := strings.Split(pathStr, "/")
		curr := folders
		for _, pathPart := range pathParts {
			if pathPart == "." || pathPart == "" {
				continue
			}
			var size int64
			var dir bool
			if pathPart == pathParts[len(pathParts)-1] {
				size = fData.Size
			} else {
				dir = true
			}
			curr = curr.AddSub(pathPart, dir, size)
		}
		return false, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	tw := table.NewWriter()
	header := table.Row{}
	for _, field := range folderFields {
		header = append(header, field)
	}
	tw.AppendHeader(header)
	for elem := range folders.ElementIterator {
		//fmt.Println(elem)
		if !elem.IsDir() {
			continue
		}
		size, fileCount, folderCount := elem.SubFolderHierarchyAggregation()
		tw.AppendRow(table.Row{fileCount, folderCount, size, human.Bytes(uint64(size)), elem.ClearString()})

		if err := output.Write([]any{fileCount, folderCount, size, human.Bytes(uint64(size)), elem.ClearString()}, map[string]interface{}{
			folderFields[0]: fileCount,
			folderFields[1]: folderCount,
			folderFields[2]: size,
			folderFields[3]: human.Bytes(uint64(size)),
			folderFields[4]: elem.ClearString(),
		}); err != nil {
			logger.Error().Err(err).Msg("cannot write output")
			defer os.Exit(1)
			return
		}
	}
	tw.SetIndexColumn(5)
	tw.SetColumnConfigs([]table.ColumnConfig{{Number: 4, Align: text.AlignRight, AlignHeader: text.AlignRight}})
	tw.SetTitle("Folder statistics")
	if consoleIndexFolderFlag {
		fmt.Println(tw.Render())
	}
	return
}
