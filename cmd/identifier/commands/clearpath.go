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

var clearPathRenameFlag bool
var clearPathAutoFlag bool
var clearPathRegexpFlag string
var clearPathRegexpReplaceFlag string

var clearPathRegexp *regexp.Regexp

// clearpathCmd renames files and folders to sanitize invalid or problematic filenames.
var clearpathCmd = &cobra.Command{
	Use:     "clearpath [path to data]",
	Aliases: []string{},
	Short:   "makes 'ugly' file- and foldernames 'pretty'",
	Long: `Renames file- and foldernames according to archival sanitization rules.

Rule details:
#1 Replace [\u0000-\u001F\u007F\n\r\t*:<>|{}] with '_'
#2 Remove [\uE000-\uF8FF] (Unicode private use area)
#3 Strip leading and trailing whitespace/special characters
#4 Replace single quotes and backticks with standard quotes
#5 Replace typographic double quotes [“”] with standard quotes "
#6 Replace multiple consecutive spaces with a single space
#7 Replace em-dash "—" with '-'
#8 Replace leading '.' with '_' and leading '~' with '-'

Uses depth-first (bottom-up) traversal to ensure child files and subdirectories
are renamed before their parent folders without path collision or broken references.

Caveat: dry-run (omitting --rename) is always recommended before executing filesystem modifications.`,
	Example: appname + ` clearpath C:/daten/aiptest --auto
` + appname + ` clearpath C:/daten/aiptest --auto --rename
` + appname + ` clearpath C:/daten/aiptest --regexp "\.\." --replace "." --rename`,
	Args: cobra.ExactArgs(1),
	Run:  doClearpath,
}

func clearpathInit() {
	clearpathCmd.Flags().BoolVar(&clearPathAutoFlag, "auto", false, "use built-in archival filename sanitization rules")
	clearpathCmd.Flags().BoolVar(&clearPathRenameFlag, "rename", false, "renames the files on filesystem (if omitted, performs a dry-run preview)")
	clearpathCmd.Flags().StringVar(&clearPathRegexpFlag, "regexp", "", "use custom regular expression pattern for renaming")
	clearpathCmd.Flags().StringVar(&clearPathRegexpReplaceFlag, "replace", "", "replacement string for custom regular expression matches")
	clearpathCmd.MarkFlagsRequiredTogether("regexp", "replace")
	clearpathCmd.MarkFlagsOneRequired("auto", "regexp")
}

func doClearpath(cmd *cobra.Command, args []string) {
	if clearPathRegexpFlag != "" {
		var err error
		clearPathRegexp, err = regexp.Compile(clearPathRegexpFlag)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", clearPathRegexpFlag)
			defer os.Exit(1)
			return
		}
		fmt.Printf("#including regexp \"%s\"\n", clearPathRegexpFlag)
	}

	dataPath, err := identifier.Fullpath(args[0])
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get full path for '%s'", args[0])
		defer os.Exit(1)
		return
	}
	logger.Info().Msgf("data path: '%s'", dataPath)
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	if !clearPathRenameFlag {
		logger.Info().Msg("dry-run: no files will be renamed")
	}
	logger.Info().Msgf("working on folder '%s'", dataPath)
	dirFS := os.DirFS(dataPath)
	pathElements, err := identifier.BuildPath(dirFS, logger)
	cobra.CheckErr(errors.Wrapf(err, "cannot build path '%s'", dataPath))

	for name, newName := range pathElements.ClearIterator(clearPathAutoFlag, clearPathRegexp, clearPathRegexpReplaceFlag) {
		if name == newName {
			continue
		}
		fmt.Printf("    %s\n--> %s\n\n", name, newName)
		if clearPathRenameFlag {
			fullpath := filepath.Join(dataPath, name)
			newpath := filepath.Join(dataPath, newName)
			logger.Info().Msgf("renaming '%s' to '%s'", fullpath, newpath)
			if err := os.Rename(fullpath, newpath); err != nil {
				logger.Error().Err(err).Msgf("cannot rename '%s' to '%s'", fullpath, newpath)
			}
		}
	}
	return
}
