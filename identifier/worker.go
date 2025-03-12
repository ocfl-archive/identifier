package identifier

import (
	"emperror.dev/errors"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
	"github.com/ocfl-archive/indexer/v3/pkg/util"
	"github.com/tealeg/xlsx/v3"
	"golang.org/x/exp/slices"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var serialWriterLock sync.Mutex

func JsonlWriteLine(w io.Writer, fData *FileData) error {
	d, err := json.Marshal(fData)
	if err != nil {
		return errors.Wrapf(err, "cannot marshal data")
	}
	serialWriterLock.Lock()
	defer serialWriterLock.Unlock()
	if _, err := w.Write(append(d, []byte("\n")...)); err != nil {
		return errors.Wrapf(err, "cannot write to output")
	}
	return nil
}

var xlsxWriterLock sync.Mutex

func XlsxWriteLine(sheet *xlsx.Sheet, fData *FileData) error {
	csvWriterlock.Lock()
	defer csvWriterlock.Unlock()
	dupStr := "no"
	if fData.Duplicate {
		dupStr = "yes"
	}
	row := sheet.AddRow()
	cell := row.AddCell()
	cell.SetString(fData.Path)
	cell = row.AddCell()
	cell.SetString(fData.Folder)
	cell = row.AddCell()
	cell.SetString(fData.Basename)
	cell = row.AddCell()
	cell.SetInt64(int64(fData.Indexer.Size))
	cell = row.AddCell()
	cell.SetDateTime(time.Unix(fData.LastMod, 0))
	cell = row.AddCell()
	cell.SetString(dupStr)
	cell = row.AddCell()
	cell.SetString(fData.Indexer.Mimetype)
	cell = row.AddCell()
	cell.SetString(fData.Indexer.Pronom)
	cell = row.AddCell()
	cell.SetString(fData.Indexer.Type)
	cell = row.AddCell()
	cell.SetString(fData.Indexer.Subtype)
	cell = row.AddCell()
	cell.SetString(fData.Indexer.Checksum[string(checksum.DigestSHA512)])
	cell = row.AddCell()
	cell.SetInt64(int64(fData.Indexer.Width))
	cell = row.AddCell()
	cell.SetInt64(int64(fData.Indexer.Height))
	cell = row.AddCell()
	cell.SetInt64(int64(fData.Indexer.Duration))
	return nil
}

var csvWriterlock sync.Mutex

func CsvWriteLine(csvWriter *csv.Writer, fData *FileData) error {
	dupStr := "no"
	if fData.Duplicate {
		dupStr = "yes"
	}
	csvWriterlock.Lock()
	defer csvWriterlock.Unlock()
	return errors.WithStack(csvWriter.Write(
		[]string{
			fData.Path,
			fData.Folder,
			fData.Basename,
			fmt.Sprintf("%v", fData.Indexer.Size),
			time.Unix(fData.LastMod, 0).Format(time.RFC3339),
			dupStr,
			fData.Indexer.Mimetype,
			fData.Indexer.Pronom,
			fData.Indexer.Type,
			fData.Indexer.Subtype,
			fData.Indexer.Checksum[string(checksum.DigestSHA512)],
			fmt.Sprintf("%v", fData.Indexer.Width),
			fmt.Sprintf("%v", fData.Indexer.Height),
			fmt.Sprintf("%v", fData.Indexer.Duration),
		}))
}

func WriteConsole(logger zLogger.ZLogger, fData *FileData, id uint, basePath string, cached bool) {
	var cachedStr string
	if cached {
		cachedStr = " [cached]"
	}
	basePath = strings.TrimSuffix(basePath, "/")
	p := path.Join(basePath, fData.Path)
	logger.Debug().Msgf("#%03d:%s %s\n           [%s] - %s\n", id, cachedStr, p, fData.Indexer.Mimetype, fData.Indexer.Checksum[string(checksum.DigestSHA512)])
	if fData.Indexer.Type == "image" && fData.Indexer.Width > 0 {
		logger.Debug().Msgf("#           image: %vx%v\n", fData.Indexer.Width, fData.Indexer.Height)
	}
}

var hashes = []string{}
var hashLock sync.Mutex

func isDup(t string) bool {
	hashLock.Lock()
	defer hashLock.Unlock()
	i, found := slices.BinarySearch(hashes, t) // find slot
	if found {
		return true // already in slice
	}
	// Make room for new value and add it
	hashes = append(hashes, *new(string))
	copy(hashes[i+1:], hashes[i:])
	hashes[i] = t
	return false
}

type FileData struct {
	Path      string            `json:"path"`
	Folder    string            `json:"folder"`
	Basename  string            `json:"basename"`
	Size      int64             `json:"size"`
	Duplicate bool              `json:"duplicate"`
	LastMod   int64             `json:"lastmod"`
	Indexer   *indexer.ResultV2 `json:"indexer"`
	LastSeen  int64             `json:"lastseen"`
}

func Worker(id uint, fsys fs.FS, actions []string, idx *util.Indexer, logger zLogger.ZLogger, jobs <-chan string, results chan<- string, badgerDB *badger.DB, startTime int64, waiter *sync.WaitGroup) {
	for path := range jobs {
		finfo, err := fs.Stat(fsys, path)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot stat (%s)%s", fsys, path)
			waiter.Done()
			return
		}
		if finfo.IsDir() {
			logger.Error().Err(err).Msgf("cannot index (%s)%s: is a directory", fsys, path)
			waiter.Done()
			return
		}

		var fData *FileData
		var fromCache bool
		if badgerDB != nil {
			err := badgerDB.View(func(txn *badger.Txn) error {
				key := []byte("file:" + path)
				if data, err := txn.Get(key); err != nil {
					if !errors.Is(err, badger.ErrKeyNotFound) {
						return errors.Wrapf(err, "cannot read from badger db")
					}
				} else {
					fData = &FileData{}
					data.Value(func(val []byte) error {
						logger.Info().Msgf("%03d loading from cache '%s'", id, path)
						if err := json.Unmarshal(val, fData); err != nil {
							return errors.Wrapf(err, "cannot unmarshal data")
						}
						fData.LastSeen = startTime
						fData.Duplicate = fData.Size > 0 && isDup(fData.Indexer.Checksum[string(checksum.DigestSHA512)])
						if err := badgerDB.Update(func(txn *badger.Txn) error {
							//key := []byte("file:"+path)
							value, err := json.Marshal(fData)
							if err != nil {
								return errors.Wrapf(err, "cannot marshal result")
							}
							if err := txn.Set(key, value); err != nil {
								return errors.Wrapf(err, "cannot write to badger db")
							}
							return nil
						}); err != nil {
							logger.Error().Err(err).Msgf("cannot write to badger db")
						}
						return nil
					})
				}
				return nil
			})
			if err != nil {
				logger.Error().Err(err).Msgf("cannot read from badger db")
			} else {
				if fData != nil {
					if fData.Size != finfo.Size() || fData.LastMod != finfo.ModTime().Unix() {
						fData = nil
					} else {
						fromCache = true
					}
				}
			}
		}

		if fData == nil {
			slices.Sort(actions)
			actions = slices.Compact(actions)
			logger.Info().Msgf("%03d indexing '%s'", id, path)
			r, cs, err := idx.Index(fsys, path, "", actions, []checksum.DigestAlgorithm{checksum.DigestSHA512}, io.Discard, logger)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot index (%s)%s", fsys, path)
				waiter.Done()
				return
			}
			if len(r.Checksum) == 0 {
				r.Checksum = make(map[string]string)
				for alg, c := range cs {
					r.Checksum[string(alg)] = c
				}
			}
			dup := r.Size > 0 && isDup(cs[checksum.DigestSHA512])
			fData = &FileData{
				path,
				filepath.Dir(path),
				filepath.Base(path),
				int64(r.Size),
				dup,
				finfo.ModTime().Unix(),
				r,
				startTime,
			}
		}

		basePath := fmt.Sprintf("%v", fsys)
		WriteConsole(logger, fData, id, basePath, fromCache)

		if badgerDB != nil {
			if err := badgerDB.Update(func(txn *badger.Txn) error {
				key := []byte("file:" + path)
				value, err := json.Marshal(fData)
				if err != nil {
					return errors.Wrapf(err, "cannot marshal result")
				}
				if err := txn.Set(key, value); err != nil {
					return errors.Wrapf(err, "cannot write to badger db")
				}
				return nil
			}); err != nil {
				logger.Error().Err(err).Msgf("cannot write to badger db")
			}
		}

		results <- fmt.Sprintf("#%03d: %s done", id, path)
		waiter.Done()
	}
}

func IterateBadger(logger zLogger.ZLogger, emptyFlag bool, duplicateFlag bool, removeFlag bool, regex *regexp.Regexp, jsonlWriter *os.File, csvWriter *csv.Writer, sheet *xlsx.Sheet, badgerDB *badger.DB, startTime int64) error {
	var removeList = [][]byte{}
	if err := badgerDB.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = []byte("file:")
		options.PrefetchValues = true
		iter := txn.NewIterator(options)
		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			k := item.Key()
			if err := item.Value(func(v []byte) error {
				logger.Debug().Msg(strings.TrimPrefix(string(k), "file:"))
				fData := &FileData{}
				if err := json.Unmarshal(v, fData); err != nil {
					return errors.Wrapf(err, "cannot unmarshal value")
				}
				var hit bool
				hit = (emptyFlag && fData.Size == 0) ||
					(duplicateFlag && fData.Duplicate) ||
					(regex != nil && regex.MatchString(fData.Basename)) ||
					(!emptyFlag && !duplicateFlag && regex == nil)
				if hit {
					logger.Info().Msgf("found %s", fData.Path)
					if jsonlWriter != nil {
						if err := JsonlWriteLine(jsonlWriter, fData); err != nil {
							logger.Error().Err(err).Msgf("cannot write to output")
						}
					}
					if csvWriter != nil {
						CsvWriteLine(csvWriter, fData)
					}
					if sheet != nil {
						XlsxWriteLine(sheet, fData)
					}
					if removeFlag {
						if err := os.Remove(fData.Path); err != nil {
							logger.Error().Err(err).Msgf("cannot remove file %s", fData.Path)
						} else {
							logger.Info().Msgf("removed file %s", fData.Path)
							removeList = append(removeList, k)
						}
					}
				}
				return nil
			}); err != nil {
				return errors.Wrapf(err, "cannot get data for key %s", string(k))
			}
		}
		if len(removeList) > 0 {
			if err := badgerDB.Update(func(txn *badger.Txn) error {
				var errs = []error{}
				for _, k := range removeList {
					if err := txn.Delete(k); err != nil {
						errs = append(errs, errors.Wrapf(err, "cannot delete key %s", string(k)))
					}
				}
				return errors.Combine(errs...)
			}); err != nil {
				return errors.Wrapf(err, "cannot delete keys")
			}
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "cannot iterate badger db")
	}
	return nil
}
