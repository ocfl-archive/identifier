package commands

import (
	"encoding/csv"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	human "github.com/dustin/go-humanize"
	"github.com/je4/identifier/identifier"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"github.com/tealeg/xlsx/v3"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// var dbFolderFlag string
// var csvFlag string
// var jsonlFlag string
// var xlsxFlag string
var emptyFlag bool
var regexpFlag string
var duplicatesFlag bool
var removeFlag bool
var typesFlag bool
var foldersFlag bool

//var fields = []string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"}

var indexListCmd = &cobra.Command{
	Use:     "list [path to data]",
	Aliases: []string{},
	Short:   "get technical metadata from database",
	Long: `get technical metadata from database
`,
	Example: `Show folder and type statistics
Show logging entries up to WARN level.

` + appname + ` --log-level WARN index list --database c:\temp\indexListerbadger --types --folders
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
	Args: nil,
	Run:  doindexList,
}

func indexListInit() {
	indexListCmd.Flags().StringVar(&dbFolderFlag, "database", "", "folder for database (must already exist)")
	indexListCmd.Flags().StringVar(&csvFlag, "csv", "", "write indexList to csv file")
	indexListCmd.Flags().StringVar(&jsonlFlag, "jsonl", "", "write indexList to jsonl file")
	indexListCmd.Flags().StringVar(&xlsxFlag, "xlsx", "", "write indexList to xlsx file (needs memory)")
	indexListCmd.Flags().BoolVar(&emptyFlag, "empty", false, "include empty files")
	indexListCmd.Flags().StringVar(&regexpFlag, "regexp", "", "include files matching regular expression")
	indexListCmd.Flags().BoolVar(&duplicatesFlag, "duplicates", false, "include duplicate files")
	indexListCmd.Flags().BoolVar(&removeFlag, "remove", false, "remove included files - requires at least one of empty, duplicate or regexp flag")
	indexListCmd.Flags().BoolVar(&typesFlag, "types", false, "show type statistics")
	indexListCmd.Flags().BoolVar(&foldersFlag, "folders", false, "show folders with aggregated size and count")
	indexListCmd.Flags().BoolVar(&consoleFlag, "console", false, "write index to console")
	indexListCmd.MarkFlagRequired("database")
}

func doindexList(cmd *cobra.Command, args []string) {
	var dataPath string
	var err error
	if dataPath == "" && dbFolderFlag == "" {
		logger.Error().Msg("either data path or database folder must be set")
		defer os.Exit(1)
		return
	}
	if removeFlag && !(emptyFlag || duplicatesFlag || regexpFlag != "") {
		logger.Error().Msg("remove flag requires at least one of empty, duplicate or regexp flag")
		defer os.Exit(1)
		return
	}

	var badgerDB *badger.DB
	var csvFile *os.File
	var csvWriter *csv.Writer
	var jsonlFile *os.File
	var sheet *xlsx.Sheet
	var regex *regexp.Regexp
	if regexpFlag != "" {
		regex, err = regexp.Compile(regexpFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", regexpFlag)
			defer os.Exit(1)
			return
		}
		fmt.Printf("#including regexp \"%s\"\n", regexpFlag)
	}
	if emptyFlag {
		fmt.Println("#including empty files")
	}
	if duplicatesFlag {
		fmt.Println("#including duplicate files")
	}
	if removeFlag {
		fmt.Println("#removing files")
	}
	if !emptyFlag && !duplicatesFlag && regexpFlag == "" {
		fmt.Println("#including all files")
	}
	if dbFolderFlag != "" {
		if badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderFlag).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
			logger.Error().Err(err).Msgf("cannot open badger database in '%s'", dbFolderFlag)
			defer os.Exit(1)
			return
		}
		defer badgerDB.Close()
	}
	if csvFlag != "" {
		if csvFile, err = os.Create(csvFlag); err != nil {
			logger.Error().Err(err).Msgf("cannot create csv file '%s'", csvFlag)
			defer os.Exit(1)
			return
		}
		defer csvFile.Close()
		csvWriter = csv.NewWriter(csvFile)
		defer csvWriter.Flush()
		csvWriter.Write(fields)
	}
	if jsonlFlag != "" {
		if jsonlFile, err = os.Create(jsonlFlag); err != nil {
			logger.Error().Err(err).Msgf("cannot create jsonl file '%s'", jsonlFlag)
			defer os.Exit(1)
			return
		}
		defer jsonlFile.Close()
	}
	if xlsxFlag != "" {
		// check, whether a file can be created
		xlsxFile, err := os.Create(xlsxFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot create xlsx file '%s'", xlsxFlag)
			defer os.Exit(1)
			return
		}
		if err := xlsxFile.Close(); err != nil {
			logger.Error().Err(err).Msgf("cannot close xlsx file '%s'", xlsxFlag)
			defer os.Exit(1)
			return
		}
		if err := os.Remove(xlsxFlag); err != nil {
			logger.Error().Err(err).Msgf("cannot remove xlsx file '%s'", xlsxFlag)
			defer os.Exit(1)
			return
		}
		xlsxWriter := xlsx.NewFile()
		sheet, err = xlsxWriter.AddSheet("indexList")
		defer func() {
			if err := xlsxWriter.Save(xlsxFlag); err != nil {
				logger.Error().Err(err).Msgf("cannot save xlsx file '%s'", xlsxFlag)
			}
		}()
		row, err := sheet.AddRowAtIndex(0)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot add header row to xlsx file '%s'", xlsxFlag)
			defer os.Exit(1)
			return
		}
		for _, field := range fields {
			cell := row.AddCell()
			cell.SetString(field)
			style := xlsx.NewStyle()
			style.Alignment.Horizontal = "center"
			style.Fill.BgColor = "0A0A0A00"
			style.Border = *xlsx.NewBorder("thin", "thin", "thin", "thick")
			cell.SetStyle(style)
		}
	}

	if sheet == nil && csvWriter == nil && jsonlFile == nil && !typesFlag && !foldersFlag {
		consoleFlag = true
	}

	var pronomSize = map[string]int64{}
	var pronomCount = map[string]int64{}
	var mimetypeSize = map[string]int64{}
	var mimetypeCount = map[string]int64{}
	var folders = identifier.NewPathElement("", true, 0, nil)
	if err := identifier.IterateBadger(logger, emptyFlag, duplicatesFlag, removeFlag, regex, jsonlFile, csvWriter, sheet, consoleFlag, badgerDB, func(fData *identifier.FileData) bool {
		var hit bool
		hit = (emptyFlag && fData.Size == 0) ||
			(duplicatesFlag && fData.Duplicate) ||
			(regex != nil && regex.MatchString(fData.Basename)) ||
			(!emptyFlag && !duplicatesFlag && regex == nil)
		if hit {
			if _, ok := pronomSize[fData.Indexer.Pronom]; !ok {
				pronomSize[fData.Indexer.Pronom] = 0
				pronomCount[fData.Indexer.Pronom] = 0
			}
			pronomSize[fData.Indexer.Pronom] += fData.Size
			pronomCount[fData.Indexer.Pronom]++
			if _, ok := mimetypeSize[fData.Indexer.Mimetype]; !ok {
				mimetypeSize[fData.Indexer.Mimetype] = 0
				mimetypeCount[fData.Indexer.Mimetype] = 0
			}
			mimetypeSize[fData.Indexer.Mimetype] += fData.Size
			mimetypeCount[fData.Indexer.Mimetype]++

			if foldersFlag {
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
			}
		}
		return hit
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	if foldersFlag {
		tw := table.NewWriter()
		tw.AppendHeader(table.Row{"Files", "Folders", "Size (Bytes)", "Size", "Folder"})
		for elem := range folders.ElementIterator {
			//fmt.Println(elem)
			if !elem.IsDir() {
				continue
			}
			size, fileCount, folderCount := elem.SubFolderHierarchyAggregation()
			tw.AppendRow(table.Row{fileCount, folderCount, size, human.Bytes(uint64(size)), elem.ClearString()})
		}
		tw.SetIndexColumn(5)
		tw.SetColumnConfigs([]table.ColumnConfig{{Number: 4, Align: text.AlignRight, AlignHeader: text.AlignRight}})
		tw.SetTitle("Folder statistics")
		fmt.Println(tw.Render())
	}
	if typesFlag {
		var totalSize int64
		var totalCount int64

		tw := table.NewWriter()
		tw.AppendHeader(table.Row{"Pronom", "Count", "Size (Bytes)", "Size"})
		for pronom, count := range pronomCount {
			size := pronomSize[pronom]
			totalSize += size
			totalCount += count
			tw.AppendRow(table.Row{pronom, count, size, human.Bytes(uint64(size))})
		}
		tw.AppendSeparator()
		tw.AppendRow(table.Row{"Total", totalCount, totalSize, human.Bytes(uint64(totalSize))})
		tw.SetIndexColumn(1)
		tw.SetTitle("Pronom statistics")
		tw.SetColumnConfigs([]table.ColumnConfig{{Number: 4, Align: text.AlignRight, AlignHeader: text.AlignRight}})
		fmt.Println(tw.Render())

		totalSize = 0
		totalCount = 0
		tw = table.NewWriter()
		tw.AppendHeader(table.Row{"Mimetype", "Count", "Size (Bytes)", "Size"})
		for mimetype, count := range mimetypeCount {
			size := mimetypeSize[mimetype]
			totalSize += size
			totalCount += count
			tw.AppendRow(table.Row{mimetype, count, size, human.Bytes(uint64(size))})
		}
		tw.AppendSeparator()
		tw.AppendRow(table.Row{"Total", totalCount, totalSize, human.Bytes(uint64(totalSize))})
		tw.SetIndexColumn(1)
		tw.SetTitle("Mimetype statistics")
		tw.SetColumnConfigs([]table.ColumnConfig{{Number: 4, Align: text.AlignRight, AlignHeader: text.AlignRight}})
		fmt.Println(tw.Render())
	}
	return
}
