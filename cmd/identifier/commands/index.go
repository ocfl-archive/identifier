package commands

import (
	"emperror.dev/errors"
	"encoding/csv"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/identifier/identifier"
	"github.com/ocfl-archive/indexer/v3/pkg/util"
	"github.com/spf13/cobra"
	"github.com/tealeg/xlsx/v3"
	"io/fs"
	"os"
	"regexp"
	"sync"
	"time"
)

var dbFolderFlag string
var csvFlag string
var jsonlFlag string
var xlsxFlag string
var concurrentFlag uint
var actionsFlag []string
var emptyFlag bool
var regexpFlag string
var duplicateFlag bool
var removeFlag bool

var fields = []string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"}

var indexCmd = &cobra.Command{
	Use:     "index [path to data]",
	Aliases: []string{},
	Short:   "retrieves technical metadata from files",
	Long: `retrieves technical metadata from files
Persistent output can be written to a badger database, which will allow additional operations without reindexing the files.
`,
	Example: `Index everything and find duplicates. Write duplicates to csv and xlsx file

` + appname + ` --log-level INFO index C:\daten\aiptest0 --database c:\temp\indexerbadger --xlsx c:/temp/identify.xlsx --csv c:/temp/identify.csv --duplicate"
2025-03-12T17:25:38+01:00 INF indexer action siegfried added timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:38+01:00 INF indexer action xml added timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:38+01:00 INF 001 indexing 'fulltext_5693376.xml' timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:38+01:00 INF 002 indexing 'info.json' timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:38+01:00 INF 003 indexing 'meta/random_mets_xxx.xml' timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
[...]
2025-03-12T17:25:39+01:00 INF 002 indexing 'payload/video/together_01_excerpt.mov' timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:39+01:00 INF 003 indexing 'payload/video/together_01_excerpt.mp4' timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"
2025-03-12T17:25:40+01:00 INF found payload/image/IMG_6914.bmp timestamp="2025-03-12 17:25:38.2009956 +0100 CET m=+0.169177501"


Index everything and write all metadata to csv and jsonl file

` + appname + ` --log-level INFO index C:\daten\aiptest0 --database c:\temp\indexerbadger --jsonl c:/temp/identify.jsonl --csv c:/temp/identify.csv
2025-03-12T17:29:37+01:00 INF indexer action siegfried added timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF indexer action xml added timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF 002 loading from cache 'fulltext_5693376.xml' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
badger 2025/03/12 17:29:37 INFO: All 0 tables opened in 0s
badger 2025/03/12 17:29:37 INFO: Discard stats nextEmptySlot: 0
badger 2025/03/12 17:29:37 INFO: Set nextTxnTs to 40
2025-03-12T17:29:37+01:00 INF 003 loading from cache 'meta/random_mets_xxx.xml' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF 001 loading from cache 'info.json' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF 002 loading from cache 'meta/random_premis_xxx.xml' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
[...]
2025-03-12T17:29:37+01:00 INF 002 loading from cache 'payload/video/together_01_excerpt.mov' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF 003 loading from cache 'payload/video/together_01_excerpt.mp4' timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF found fulltext_5693376.xml timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF found info.json timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF found meta/random_mets_xxx.xml timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF found meta/random_premis_xxx.xml timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
2025-03-12T17:29:37+01:00 INF found meta/schemas/MARC21slim.xsd timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
[...]
2025-03-12T17:29:37+01:00 INF found payload/video/together_01_excerpt.mp4 timestamp="2025-03-12 17:29:37.3280156 +0100 CET m=+0.129022001"
`,
	Args: cobra.MaximumNArgs(1),
	Run:  doIndex,
}

func indexInit() {
	indexCmd.Flags().StringVar(&dbFolderFlag, "database", "", "folder for database (must already exist)")
	indexCmd.Flags().StringVar(&csvFlag, "csv", "", "write index to csv file")
	indexCmd.Flags().StringVar(&jsonlFlag, "jsonl", "", "write index to jsonl file")
	indexCmd.Flags().StringVar(&xlsxFlag, "xlsx", "", "write index to xlsx file (needs memory)")
	indexCmd.Flags().UintVarP(&concurrentFlag, "concurrent", "n", 3, "number of concurrent workers")
	indexCmd.Flags().StringSliceVar(&actionsFlag, "actions", []string{"siegfried", "xml"}, "actions to be performed")
	indexCmd.Flags().BoolVar(&emptyFlag, "empty", false, "include empty files")
	indexCmd.Flags().StringVar(&regexpFlag, "regexp", "", "include files matching regular expression")
	indexCmd.Flags().BoolVar(&duplicateFlag, "duplicate", false, "include duplicate files")
	indexCmd.Flags().BoolVar(&removeFlag, "remove", false, "remove included files - requires at least one of empty, duplicate or regexp flag")
}

func doIndex(cmd *cobra.Command, args []string) {
	dataPath, err := identifier.Fullpath(args[0])
	cobra.CheckErr(err)
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	if removeFlag && !(emptyFlag || duplicateFlag || regexpFlag != "") {
		logger.Error().Msg("remove flag requires at least one of empty, duplicate or regexp flag")
		defer os.Exit(1)
		return
	}

	idx, err := util.InitIndexer(conf.Indexer, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot initialize indexer")
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
	}
	if dbFolderFlag != "" {
		if badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderFlag).WithCompression(badgerOptions.Snappy)); err != nil {
			logger.Error().Err(err).Msgf("cannot open badger database in '%s'", dbFolderFlag)
			defer os.Exit(1)
			return
		}
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
		sheet, err = xlsxWriter.AddSheet("index")
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

	startTime := time.Now().Unix()
	if dataPath != "" {
		dirFS := os.DirFS(dataPath)
		jobs := make(chan string, 100)
		results := make(chan string, 100)

		var waiter = &sync.WaitGroup{}

		for w := uint(1); w <= concurrentFlag; w++ {
			go identifier.Worker(
				w,
				dirFS,
				actionsFlag,
				idx,
				logger,
				jobs,
				results,
				badgerDB,
				startTime,
				waiter,
			)
		}

		go func() {
			for n := range results {
				logger.Debug().Msgf("result: %s", n)
			}
		}()

		if err := fs.WalkDir(dirFS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return errors.Wrapf(err, "cannot walk %s/%s", dirFS, path)
			}
			if d.IsDir() {
				logger.Debug().Msgf("folder %s/%s\n", dirFS, path)
				return nil
			}
			//			logger.Info().Msgf("[f] %s/%s\n", dirFS, path)

			waiter.Add(1)
			jobs <- path

			return nil
		}); err != nil {
			panic(fmt.Errorf("cannot walkd folder %v: %v", dirFS, err))
		}

		waiter.Wait()
		close(jobs)
	}
	if err := identifier.IterateBadger(logger, emptyFlag, duplicateFlag, removeFlag, regex, jsonlFile, csvWriter, sheet, badgerDB, startTime); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	return
}
