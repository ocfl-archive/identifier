package commands

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"regexp"
)

var clearPathRenameFlag bool
var clearPathAutoFlag bool
var clearPathRegexpFlag string
var clearPathRegexpReplaceFlag string

var clearPathRegexp *regexp.Regexp

var clearpathCmd = &cobra.Command{
	Use:     "clearpath [path to data]",
	Aliases: []string{},
	Short:   "makes 'ugly' file- and foldernames 'pretty'",
	Long: `renames file- and foldernames according to the following rules.

#1 replace [\u0000-\u001F\u007F\n\r\t*:<>|{}] with '_'
#2 remove [\uE000-\uF8FF] (private use area)
#3 "^[~\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200f\u2028\u2029\u202f\u205f\u3000]*(.*?)[\u0009\u000a-\u000d\u0020\u0085\u00a0\u1680\u2000-\u20a0\u2028\u2029\u202f\u205f\u3000]*$" (leading/trailing spaces and ~)
#4 replace single quotes and backtick with '
#5 replace [“”] (double quotes) with "
#6 replace multiple spaces with single space
#7 replace special dash character "—" with '-'
#8 replace leading '.' with '_'
#8 replace leading '~' with '-'

This function uses the deep-first search algorithm to rename files 
and folders in the given path to make sure, that there are no conflicts. 


Caveat: dry-run (no --rename flag) is always recommended before renaming files on filesystem.
`,
	Example: appname + ` clearpath C:/daten/aiptest
2025-03-12T11:03:21+01:00 INF dry-run: no files will be renamed timestamp="2025-03-12 11:03:21.1223634 +0100 CET m=+0.156712501"
2025-03-12T11:03:21+01:00 INF working on folder 'C:/daten/aiptest' timestamp="2025-03-12 11:03:21.1223634 +0100 CET m=+0.156712501"
    payload/#1    test[0]/.test{3}.txt
--> payload/#1    test[0]/_test_3_.txt

    payload/#1    test[0]/~test.“new”.txt
--> payload/#1    test[0]/-test."new".txt

    payload/#1    test[0]/~~test.txt
--> payload/#1    test[0]/-~test.txt

    payload/#1    test[0]
--> payload/#1 test[0]`,
	Args: cobra.ExactArgs(1),
	Run:  doClearpath,
}

func clearpathInit() {
	clearpathCmd.Flags().BoolVar(&clearPathAutoFlag, "auto", false, "use built in rename rules")
	clearpathCmd.Flags().BoolVar(&clearPathRenameFlag, "rename", false, "renames the files on filesystem (if not set it's just a dry run)")
	clearpathCmd.Flags().StringVar(&clearPathRegexpFlag, "regexp", "", "use custom regexp for renaming")
	clearpathCmd.Flags().StringVar(&clearPathRegexpReplaceFlag, "replace", "", "replace characters for regexp")
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
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	if !clearPathRenameFlag {
		logger.Info().Msg("dry-run: no files will be renamed")
	}
	logger.Info().Msgf("working on folder '%s'", dataPath)
	dirFS := os.DirFS(dataPath)
	pathElements, err := identifier.BuildPath(dirFS)
	cobra.CheckErr(errors.Wrapf(err, "cannot build path '%s'", dataPath))

	for name, newName := range pathElements.ClearIterator(clearPathAutoFlag, clearPathRegexp, clearPathRegexpReplaceFlag) {
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
