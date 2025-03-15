package commands

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"regexp"
)

var csvIndexListFlag string
var jsonlIndexListFlag string
var xlsxIndexListFlag string
var dbFolderIndexListFlag string
var emptyIndexListFlag bool
var regexpIndexListFlag string
var duplicatesIndexListFlag bool
var prefixIndexListFlag string
var removeIndexListFlag bool
var consoleIndexListFlag bool

var fieldsIndexList = []string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"}

var indexListCmd = &cobra.Command{
	Use:     "list [path to data]",
	Aliases: []string{},
	Short:   "get technical metadata from database",
	Long: `get technical metadata from database
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
	Args: nil,
	Run:  doindexList,
}

func indexListInit() {
	indexListCmd.Flags().StringVar(&dbFolderIndexListFlag, "database", "", "folder for database (must already exist)")
	indexListCmd.Flags().StringVar(&csvIndexListFlag, "csv", "", "write indexList to csv file")
	indexListCmd.Flags().StringVar(&jsonlIndexListFlag, "jsonl", "", "write indexList to jsonl file")
	indexListCmd.Flags().StringVar(&xlsxIndexListFlag, "xlsx", "", "write indexList to xlsx file (needs memory)")
	indexListCmd.Flags().BoolVar(&emptyIndexListFlag, "empty", false, "include empty files")
	indexListCmd.Flags().StringVar(&regexpIndexListFlag, "regexp", "", "include files matching regular expression")
	indexListCmd.Flags().BoolVar(&duplicatesIndexListFlag, "duplicates", false, "include duplicate files")
	indexListCmd.Flags().StringVar(&prefixIndexListFlag, "prefix", "", "folder path prefix")
	indexListCmd.Flags().BoolVar(&removeIndexListFlag, "remove", false, "remove included files - requires at least one of empty, duplicate or regexp flag")
	indexListCmd.Flags().BoolVar(&consoleIndexListFlag, "console", false, "write index to console")
	indexListCmd.MarkFlagRequired("database")
}

func doindexList(cmd *cobra.Command, args []string) {
	var err error
	if removeIndexListFlag && !(emptyIndexListFlag || duplicatesIndexListFlag || regexpIndexListFlag != "") {
		logger.Error().Msg("remove flag requires at least one of empty, duplicate or regexp flag")
		defer os.Exit(1)
		return
	}

	var regex *regexp.Regexp
	if regexpIndexListFlag != "" {
		regex, err = regexp.Compile(regexpIndexListFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", regexpIndexListFlag)
			defer os.Exit(1)
			return
		}
		fmt.Printf("#including regexp \"%s\"\n", regexpIndexListFlag)
	}
	if emptyIndexListFlag {
		fmt.Println("#including empty files")
	}
	if duplicatesIndexListFlag {
		fmt.Println("#including duplicate files")
	}
	if prefixIndexListFlag != "" {
		fmt.Printf("#including prefix \"%s\"\n", prefixIndexListFlag)
	}
	if !emptyIndexListFlag && !duplicatesIndexListFlag && regexpIndexListFlag == "" {
		fmt.Println("#including all files")
	}
	if removeIndexListFlag {
		fmt.Println("#removing files")
	}
	output, err := identifier.NewOutput(consoleIndexListFlag || (csvIndexListFlag == "" && jsonlIndexListFlag == "" && xlsxIndexListFlag == ""), csvIndexListFlag, jsonlIndexListFlag, xlsxIndexListFlag, "list", fieldsIndexList, logger)
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

	badgerIterator, err := identifier.NewBadgerIterator(dbFolderIndexListFlag, !removeIndexListFlag, logger)
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

	if err := badgerIterator.Iterate(prefixIndexFolderFlag, func(fData *identifier.FileData) (remove bool, err error) {
		var hit bool
		hit = (emptyIndexListFlag && fData.Size == 0) ||
			(duplicatesIndexListFlag && fData.Duplicate) ||
			(regex != nil && regex.MatchString(fData.Basename)) ||
			(!emptyIndexListFlag && !duplicatesIndexListFlag && regex == nil)
		if hit {
			if err := output.Write([]any{
				fData.Path,
				fData.Folder,
				fData.Basename,
				fData.Size,
				fData.LastMod,
				fData.Duplicate,
				fData.Indexer.Mimetype,
				fData.Indexer.Pronom,
				fData.Indexer.Type,
				fData.Indexer.Subtype,
				fData.Indexer.Checksum[string(checksum.DigestSHA512)],
				fData.Indexer.Width,
				fData.Indexer.Height,
				fData.Indexer.Duration},
				fData); err != nil {
				return false, errors.Wrapf(err, "cannot write output")
			}
		}
		return removeIndexListFlag, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	return
}
