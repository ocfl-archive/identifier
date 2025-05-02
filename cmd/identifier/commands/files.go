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

var filesRemoveFlag bool
var filesRegexpFlag string

var filesCmd = &cobra.Command{
	Use:     "files [path to data]",
	Aliases: []string{},
	Short:   "list files based on go regular expression (with remove option)",
	Long: `list files based on go regular expression (https://pkg.go.dev/regexp/syntax)
There is an option to remove the files from filesystem.

Caveat: dry-run (no --remove flag) is always recommended before removing files from filesystem.
`,
	Example: `find all files with extension '.jpg' and '.gif' and remove them

` + appname + ` files C:/daten/aiptest --regexp "\.(jpg|gif)$" --remove
2025-03-12T11:36:12+01:00 INF working on folder 'C:/daten/aiptest0' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"
payload/image/IMG_6914.gif
2025-03-12T11:36:12+01:00 INF removing 'C:\daten\aiptest0\payload\image\IMG_6914.gif' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"
payload/image/IMG_6914.jpg
2025-03-12T11:36:12+01:00 INF removing 'C:\daten\aiptest0\payload\image\IMG_6914.jpg' timestamp="2025-03-12 11:36:12.2337424 +0100 CET m=+0.299511801"


list all files starting with '.' and having more than 3 characters in name

` + appname + ` files C:/daten/aiptest --regexp "^\..." 
2025-03-12T11:29:41+01:00 INF dry-run: no files will be removed timestamp="2025-03-12 11:29:41.4411178 +0100 CET m=+0.228291601"
2025-03-12T11:29:41+01:00 INF working on folder 'C:/daten/aiptest' timestamp="2025-03-12 11:29:41.4411178 +0100 CET m=+0.228291601"
payload/image/.image.txt
payload/test[0]/.test{3}.txt`,
	Args: cobra.ExactArgs(1),
	Run:  dofiles,
}

func filesInit() {
	filesCmd.Flags().StringVar(&filesRegexpFlag, "regexp", "", "[required] regular expression to match files")
	filesCmd.MarkFlagRequired("regexp")
	filesCmd.Flags().BoolVar(&filesRemoveFlag, "remove", false, "removes (deletes) the files from filesystem (if not set it's just a dry run)")
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
	pathElements, err := identifier.BuildPath(dirFS, nil)
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
