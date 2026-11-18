package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"emperror.dev/errors"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var foldersRemoveFlag bool
var foldersRegexpFlag string

// foldersCmd lists matching directories and their contents based on regular expressions, with optional recursive deletion.
var foldersCmd = &cobra.Command{
	Use:     "folders [path to data]",
	Aliases: []string{},
	Short:   "list folders including files and subfolders based on go regular expression (with remove option)",
	Long: `List folders including files and subfolders based on Go regular expression syntax (https://pkg.go.dev/regexp/syntax).
If multiple nested folders match, only the topmost matching ancestor is returned (which encompasses all children).
Provides an optional --remove flag to recursively delete matching directories from the filesystem.

Caveat: dry-run (omitting --remove) is always recommended before executing directory deletion.`,
	Example: `Find all folders with 8 or more characters in their name:
` + appname + ` folders C:/daten/aiptest --regexp ".{8,}"

Find and recursively delete all folders starting with 'empty':
` + appname + ` folders C:/daten/aiptest --regexp "^empty" --remove`,
	Args: cobra.ExactArgs(1),
	Run:  dofolders,
}

func foldersInit() {
	foldersCmd.Flags().StringVar(&foldersRegexpFlag, "regexp", "", "[required] regular expression to match folders")
	foldersCmd.MarkFlagRequired("regexp")
	foldersCmd.Flags().BoolVar(&foldersRemoveFlag, "remove", false, "removes (deletes) the matching folders and all contents from filesystem (if omitted, performs a dry-run list)")
}

func dofolders(cmd *cobra.Command, args []string) {
	dataPath, err := identifier.Fullpath(args[0])
	cobra.CheckErr(err)
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	folderRegexp, err := regexp.Compile(foldersRegexpFlag)
	cobra.CheckErr(errors.Wrapf(err, "cannot compile '%s'", foldersRegexpFlag))

	if !foldersRemoveFlag {
		logger.Info().Msg("dry-run: no files will be removed")
	}
	logger.Info().Msgf("working on folder '%s'", dataPath)
	logger.Info().Msgf("using regexp \"%s\"", foldersRegexpFlag)
	dirFS := os.DirFS(dataPath)
	pathElements, err := identifier.BuildPath(dirFS, logger)
	cobra.CheckErr(errors.Wrapf(err, "cannot build paths from '%s'", dataPath))

	for name := range pathElements.FindDirname(folderRegexp) {
		fmt.Printf("%s\n", name)
		if foldersRemoveFlag {
			fullpath := filepath.Join(dataPath, name)
			logger.Info().Msgf("removing '%s'", fullpath)
			if err := os.RemoveAll(fullpath); err != nil {
				logger.Fatal().Err(err).Msgf("cannot remove '%s'", fullpath)
			}
		}
	}
	return
}
