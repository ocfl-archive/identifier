package commands

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
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
	Example: ``,
	Args:    cobra.MaximumNArgs(1),
	Run:     doindexList,
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
	var dataPath string
	var err error
	if len(args) > 0 {
		dataPath, err = identifier.Fullpath(args[0])
		cobra.CheckErr(err)
		if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
			cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
		}
	}
	if removeIndexListFlag && dataPath == "" {
		logger.Error().Msg("remove flag requires path to data")
		defer os.Exit(1)
		return
	}
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
			if removeIndexListFlag {
				fullpath := filepath.Join(dataPath, fData.Path)
				logger.Info().Msgf("removing file '%s'", fullpath)
				if err := os.Remove(fullpath); err != nil {
					logger.Error().Err(err).Msgf("cannot remove file '%s'", fullpath)
					// return false, errors.Wrapf(err, "cannot remove file '%s'", fData.Path)
					return removeIndexListFlag, nil
				}
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		logger.Error().Err(err).Msg("cannot iterate badger")
	}
	return
}
