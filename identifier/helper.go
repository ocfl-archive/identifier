package identifier

import (
	"emperror.dev/errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func Fullpath(path string) (string, error) {
	path = filepath.ToSlash(filepath.Clean(path))
	if path == "" {
		currdir, err := os.Getwd()
		if err != nil {
			return "", errors.Wrap(err, "cannot get current directory")
		}
		return filepath.ToSlash(currdir), nil
	}
	if strings.HasPrefix(path, "/") {
		// absolute path on un*x
		if runtime.GOOS != "windows" {
			return path, nil
		}
		// UNC path
		if strings.HasPrefix(path, "//") {
			return path, nil
		}

		currdir, err := os.Getwd()
		if err != nil {
			return "", errors.Wrap(err, "cannot get current directory")
		}
		currdir = filepath.ToSlash(currdir)
		// this is a problem. current dir is unc, but path is not
		if strings.HasPrefix(currdir, "/") {
			return "", errors.Errorf("current directory '%s' is UNC path, but path '%s' is not", currdir, path)
		}
		// no drive letter in current dir
		if len(currdir) > 1 && currdir[1] != ':' {
			return "", errors.Wrapf(err, "no drive letter in current folder '%s' path with leading '/' not allowed - %s", currdir, path)
		}
		if len(currdir) > 1 {
			return filepath.ToSlash(filepath.Join(currdir[0:2], path)), nil
		}
		return path, nil
	}
	// absolute path on windows with drive letter
	if runtime.GOOS == "windows" && len(path) > 1 && path[1] == ':' {
		return path, nil
	}
	currdir, err := os.Getwd()
	if err != nil {
		return "", errors.Wrap(err, "cannot get current directory")
	}
	return filepath.ToSlash(filepath.Join(currdir, path)), nil
}
