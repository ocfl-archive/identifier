package identifier

import (
	"emperror.dev/errors"
	"encoding/json"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/utils/v2/pkg/zLogger"
	"runtime"
)

func NewBadgerIterator(dbFolderPath string, readOnly bool, logger zLogger.ZLogger) (*BadgerIterator, error) {
	if runtime.GOOS == "windows" {
		readOnly = false
	}
	var err error
	reader := &BadgerIterator{readOnly: readOnly, logger: logger}
	if dbFolderPath != "" {
		if reader.badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderPath).WithReadOnly(readOnly).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
			return nil, errors.Wrapf(err, "cannot open badger database in '%s'", dbFolderPath)
		}
		// defer badgerDB.Close()
	}
	return reader, nil
}

type BadgerIterator struct {
	badgerDB *badger.DB
	readOnly bool
	logger   zLogger.ZLogger
}

func (r *BadgerIterator) Close() error {
	if r.badgerDB != nil {
		return errors.WithStack(r.badgerDB.Close())
	}
	return nil
}

func (r *BadgerIterator) Iterate(prefix string, do func(fData *FileData) (remove bool, err error)) error {
	var removeKeys = [][]byte{}
	if err := r.badgerDB.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = true
		if prefix != "" {
			options.Prefix = []byte(prefix)
		}
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(val []byte) error {
				fData := &FileData{}
				if err := json.Unmarshal(val, fData); err != nil {
					return err
				}
				remove, err := do(fData)
				if err != nil {
					return err
				}
				if remove {
					removeKeys = append(removeKeys, k)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "cannot iterate over badger database with prefix '%s'", prefix)
	}
	if !r.readOnly {
		if err := r.badgerDB.Update(func(txn *badger.Txn) error {
			for _, k := range removeKeys {
				r.logger.Info().Msgf("removing key '%s'", k)
				if err := txn.Delete(k); err != nil {
					r.logger.Error().Err(err).Msgf("cannot remove key '%s'", k)
				}
			}
			if err := txn.Commit(); err != nil {
				r.logger.Error().Err(err).Msgf("cannot commit transaction")
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "cannot remove keys")
		}
	}
	return nil
}
