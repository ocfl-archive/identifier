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

var filesRemoveFlag bool
var filesRegexpFlag string

// filesCmd matches and lists files in a directory hierarchy according to regular expressions, with optional deletion.
var filesCmd = &cobra.Command{
	Use:     "files [path to data]",
	Aliases: []string{},
	Short:   "list files based on go regular expression (with remove option)",
	Long: `List files based on Go regular expression syntax (https://pkg.go.dev/regexp/syntax).
Provides an optional --remove flag to delete matched files directly from the filesystem.

Caveat: dry-run (omitting --remove) is always recommended before executing file deletion.`,
	Example: `find all files with extension '.jpg' and '.gif' and remove them:
` + appname + ` files C:/daten/aiptest --regexp "\.(jpg|gif)$" --remove

list all files starting with '.' (e.g. hidden or AppleDouble files):
` + appname + ` files C:/daten/aiptest --regexp "^\."`,
	Args: cobra.ExactArgs(1),
	Run:  dofiles,
}

func filesInit() {
	filesCmd.Flags().StringVar(&filesRegexpFlag, "regexp", "", "[required] regular expression to match files")
	filesCmd.MarkFlagRequired("regexp")
	filesCmd.Flags().BoolVar(&filesRemoveFlag, "remove", false, "removes (deletes) matching files from filesystem (if omitted, performs a dry-run list)")
}

func dofiles(cmd *cobra.Command, args []string) {
	dataPath, err := identifier.Fullpath(args[0])
	cobra.CheckErr(err)
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	fileRegexp, err := regexp.Compile(filesRegexpFlag)
	cobra.CheckErr(errors.Wrapf(err, "cannot compile '%s'", filesRegexpFlag))

	if !filesRemoveFlag {
		logger.Info().Msg("dry-run: no files will be removed")
	}
	logger.Info().Msgf("working on folder '%s'", dataPath)
	dirFS := os.DirFS(dataPath)
	pathElements, err := identifier.BuildPath(dirFS, logger)
	cobra.CheckErr(errors.Wrapf(err, "cannot build paths from '%s'", dataPath))

	for name := range pathElements.FindBasename(fileRegexp) {
		fmt.Printf("%s\n", name)
		if filesRemoveFlag {
			fullpath := filepath.Join(dataPath, name)
			logger.Info().Msgf("removing '%s'", fullpath)
			if err := os.Remove(fullpath); err != nil {
				logger.Fatal().Err(err).Msgf("cannot remove '%s'", fullpath)
			}
		}
	}
	return
}
