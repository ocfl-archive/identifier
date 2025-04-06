package commands

import (
	"emperror.dev/errors"
	"encoding/csv"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/identifier/identifier"
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
var consoleFlag bool
var concurrentFlag uint
var actionsFlag []string

var fields = []string{"path", "folder", "basename", "size", "lastmod", "duplicate", "mimetype", "pronom", "type", "subtype", "checksum", "width", "height", "duration"}

var indexCmd = &cobra.Command{
	Use:     "index [path to data]",
	Aliases: []string{},
	Short:   "retrieves technical metadata from files",
	Long: `retrieves technical metadata from files
Persistent output can be written to a badger database, which will allow additional operations without reindexing the files.
`,
	Example: ``,
	Args:    cobra.ExactArgs(1),
	Run:     doIndex,
}

func indexInit() {
	indexCmd.Flags().StringVar(&dbFolderFlag, "database", "", "folder for database (must already exist)")
	indexCmd.Flags().StringVar(&csvFlag, "csv", "", "write index to csv file")
	indexCmd.Flags().StringVar(&jsonlFlag, "jsonl", "", "write index to jsonl file")
	indexCmd.Flags().StringVar(&xlsxFlag, "xlsx", "", "write index to xlsx file (needs memory)")
	indexCmd.Flags().UintVarP(&concurrentFlag, "concurrent", "n", 3, "number of concurrent workers")
	indexCmd.Flags().StringSliceVar(&actionsFlag, "actions", []string{"siegfried", "xml"}, "actions to be performed")
	indexCmd.Flags().BoolVar(&consoleFlag, "console", false, "write index to console")
	indexCmd.MarkFlagDirname("database")
	indexCmd.MarkFlagFilename("jsonl", "jsonl", "json")
	indexCmd.MarkFlagFilename("csv", "csv")
	indexCmd.MarkFlagFilename("xlsx", "xlsx")

	indexListInit()
	indexFoldersInit()
	indexPronomInit()
	indexMimeInit()
	indexCmd.AddCommand(indexListCmd, indexFoldersCmd, indexPronomCmd, indexMimeCmd)

}

func doIndex(cmd *cobra.Command, args []string) {
	if len(args) == 0 && dbFolderFlag == "" {
		logger.Error().Msg("either data path or database folder must be set")
		_ = cmd.Help()
		logger.Error().Msg("either data path or database folder must be set")
		defer os.Exit(1)
		return
	}
	var dataPath string
	var err error
	if len(args) > 0 {
		dataPath, err = identifier.Fullpath(args[0])
		cobra.CheckErr(err)
		if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
			cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
		}
	}
	if removeIndexListFlag && !(emptyIndexListFlag || duplicatesIndexListFlag || regexpIndexListFlag != "") {
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
	if regexpIndexListFlag != "" {
		regex, err = regexp.Compile(regexpIndexListFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", regexpIndexListFlag)
			defer os.Exit(1)
			return
		}
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

	if sheet == nil && csvWriter == nil && jsonlFile == nil {
		consoleFlag = true
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
	if badgerDB != nil {
		if err := identifier.IterateBadger(logger, emptyIndexListFlag, duplicatesIndexListFlag, removeIndexListFlag, regex, jsonlFile, csvWriter, sheet, consoleFlag, badgerDB, func(fData *identifier.FileData) bool {
			return true
		}); err != nil {
			logger.Error().Err(err).Msg("cannot iterate badger")
		}
	}
	return
}
