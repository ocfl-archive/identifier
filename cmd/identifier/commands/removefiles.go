package commands

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/identifier/identifier"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"regexp"
)

var removeFilesRemoveFlag bool
var removeFilesRegexpFlag string

var removeFilesCmd = &cobra.Command{
	Use:     "removefiles [path to data]",
	Aliases: []string{},
	Short:   "removes files based on go regular expression",
	Long: `removes files based on go regular expression (https://pkg.go.dev/regexp/syntax)

Caveat: dry-run (no --remove flag) is always recommended before removing files from filesystem.
`,
	Example: `find all files with extension '.jpg' and '.gif' and remove them

` + appname + ` removefiles C:/daten/aiptest --regexp "\.(jpg|gif)$" --remove
2025-03-12T11:36:12+01:00 INF working on folder 'C:/daten/aiptest0' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"
payload/image/IMG_6914.gif
2025-03-12T11:36:12+01:00 INF removing 'C:\daten\aiptest0\payload\image\IMG_6914.gif' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"
payload/image/IMG_6914.jpg
2025-03-12T11:36:12+01:00 INF removing 'C:\daten\aiptest0\payload\image\IMG_6914.jpg' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"


list all files starting with '.' and having more than 3 characters in name

` + appname + ` removefiles C:/daten/aiptest --regexp "^\..." 
2025-03-12T11:29:41+01:00 INF dry-run: no files will be removed timestamp="2025-03-12 11:29:41.4411178 +0100 CET m=+0.228291601"
2025-03-12T11:29:41+01:00 INF working on folder 'C:/daten/aiptest' timestamp="2025-03-12 11:29:41.4411178 +0100 CET m=+0.228291601"
payload/image/.image.txt
payload/test[0]/.test{3}.txt`,
	Args: cobra.ExactArgs(1),
	Run:  doRemoveFiles,
}

func removeFilesInit() {
	removeFilesCmd.Flags().StringVar(&removeFilesRegexpFlag, "regexp", "", "[required] regular expression to match files")
	removeFilesCmd.MarkFlagRequired("regexp")
	removeFilesCmd.Flags().BoolVar(&removeFilesRemoveFlag, "remove", false, "removes (deletes) the files from filesystem (if not set it's just a dry run)")
}

func doRemoveFiles(cmd *cobra.Command, args []string) {
	dataPath, err := identifier.Fullpath(args[0])
	cobra.CheckErr(err)
	if fi, err := os.Stat(dataPath); err != nil || !fi.IsDir() {
		cobra.CheckErr(errors.Errorf("'%s' is not a directory", dataPath))
	}

	fileRegexp, err := regexp.Compile(removeFilesRegexpFlag)
	cobra.CheckErr(errors.Wrapf(err, "cannot compile '%s'", removeFilesRegexpFlag))

	if !removeFilesRemoveFlag {
		logger.Info().Msg("dry-run: no files will be removed")
	}
	logger.Info().Msgf("working on folder '%s'", dataPath)
	dirFS := os.DirFS(dataPath)
	pathElements, err := identifier.BuildPath(dirFS)
	cobra.CheckErr(errors.Wrapf(err, "cannot build paths from '%s'", dataPath))

	for name := range pathElements.FindBasename(fileRegexp) {
		fmt.Printf("%s\n", name)
		if removeFilesRemoveFlag {
			fullpath := filepath.Join(dataPath, name)
			logger.Info().Msgf("removing '%s'", fullpath)
			if err := os.Remove(fullpath); err != nil {
				logger.Fatal().Err(err).Msgf("cannot remove '%s'", fullpath)
			}
		}
	}
	return
}
