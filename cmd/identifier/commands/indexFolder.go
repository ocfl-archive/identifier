package commands

import (
	"fmt"
	human "github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var csvIndexFolderFlag string
var jsonlIndexFolderFlag string
var xlsxIndexFolderFlag string
var consoleIndexFolderFlag bool
var prefixIndexFolderFlag string
var dbIndexFolderFlag string

// var fields = []string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"}
var folderFields = []string{"Files", "Folders", "Bytes", "Size", "Path"}
var indexFoldersCmd = &cobra.Command{
	Use:     "folders",
	Aliases: []string{},
	Short:   "get folder statistics from database",
	Long: `get folder statistics from database
`,
	Example: `Show folder and type statistics
Show logging entries up to WARN level.

` + appname + ` --log-level WARN index list --database c:\temp\indexerbadger --types --folders
#including all files
+--------------------------------------------------------------------+
| Folder statistics                                                  |
+-------+---------+--------------+--------+--------------------------+
| FILES | FOLDERS | SIZE (BYTES) |   SIZE | FOLDER                   |
+-------+---------+--------------+--------+--------------------------+
|     1 |       1 |         7193 | 7.2 kB | /meta/schemas            |
|     3 |       2 |       239817 | 240 kB | /meta                    |
|     9 |       1 |     15042369 |  15 MB | /payload/#1    audio     |
|    10 |       1 |    172858675 | 173 MB | /payload/image           |
|     1 |       1 |            0 |    0 B | /payload/test[0]/folders |
[...]

List duplicatres metadata to csv and jsonl file and show them on console.
Show logging entries up to INFO level.

` + appname + ` --log-level INFO index list --database c:\temp\indexerbadger --jsonl c:/temp/identify.jsonl --csv c:/temp/identify.csv --console --duplicates
#including duplicate files
2025-03-13T18:08:39+01:00 INF All 4 tables opened in 1ms
 timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
2025-03-13T18:08:39+01:00 INF Discard stats nextEmptySlot: 0
 timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
2025-03-13T18:08:39+01:00 INF Set nextTxnTs to 920 timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
2025-03-13T18:08:39+01:00 INF Deleting empty file: c:\temp\indexerbadger\000063.vlog timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
2025-03-13T18:08:39+01:00 INF found payload/image/IMG_6914.bmp timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
'payload/image/IMG_6914.bmp' - 46802b2518037cd7c76a7cf121845136b0a703c5245aafbfbdd602cfe879de4ff76d8fc723f71cb056ba6401e1aaaa6dab3fc798e4ccdde654b49f51a7d0aeff
           [fmt/116 - image/bmp] 37 MB
2025-03-13T18:08:39+01:00 INF Lifetime L0 stalled for: 0s
 timestamp="2025-03-13 18:08:39.2911666 +0100 CET m=+0.111345601"
2025-03-13T18:08:39+01:00 INF
Level 0 [ ]: NumTables: 03. Size: 68 KiB of 0 B. Score: 0.00->0.00 StaleData: 0 B Target FileSize: 64 MiB
[...]`,
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
	if err := badgerIterator.Iterate(prefixIndexFolderFlag, func(fData *identifier.FileData) (remove bool, err error) {
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
