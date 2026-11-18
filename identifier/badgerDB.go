package identifier

import (
	"encoding/json/v2"
	"runtime"

	"emperror.dev/errors"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/je4/utils/v2/pkg/zLogger"
)

// NewBadgerIterator opens an existing Badger key-value database at dbFolderPath and returns
// a BadgerIterator configured for reading (and optionally deleting) indexed entries.
// On Windows platforms, readOnly is automatically forced to false due to underlying file-locking requirements.
func NewBadgerIterator(dbFolderPath string, readOnly bool, logger zLogger.ZLogger) (*BadgerIterator, error) {
	if runtime.GOOS == "windows" {
		readOnly = false
	}
	var err error
	reader := &BadgerIterator{readOnly: readOnly, logger: logger}
	if dbFolderPath != "" {
		if readOnly {
			logger.Info().Msgf("open read only badger database in '%s'", dbFolderPath)
		} else {
			logger.Info().Msgf("open read write badger database in '%s'", dbFolderPath)
		}
		if reader.badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderPath).WithReadOnly(readOnly).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
			return nil, errors.Wrapf(err, "cannot open badger database in '%s'", dbFolderPath)
		}
	}
	return reader, nil
}

// BadgerIterator wraps a BadgerDB database instance, providing typed iteration methods
// for file index records and AI-generated folder descriptions, with optional key deletion.
type BadgerIterator struct {
	badgerDB *badger.DB
	readOnly bool
	logger   zLogger.ZLogger
}

// Close gracefully closes the underlying BadgerDB instance and flushes buffers.
func (r *BadgerIterator) Close() error {
	if r.badgerDB != nil {
		return errors.WithStack(r.badgerDB.Close())
	}
	return nil
}

// IterateJSON iterates over all records whose keys match the given prefix,
// deserializing each JSON value into a new instance of T.
// The callback `do` receives the string key and a pointer to the unmarshaled entity T.
// If the callback returns remove=true and the iterator is in read-write mode, the key will be deleted.
func IterateJSON[T any](r *BadgerIterator, prefix string, do func(key string, entity *T) (remove bool, err error)) error {
	if err := r.Iterate(prefix, func(key, value []byte) (remove bool, err error) {
		var entity T
		if err := json.Unmarshal(value, &entity); err != nil {
			return false, errors.Wrapf(err, "cannot unmarshal %T for key '%s': %s", entity, string(key), string(value))
		}
		remove, err = do(string(key), &entity)
		if err != nil {
			return false, errors.Wrapf(err, "cannot iterate %T for key '%s': %s", entity, string(key), string(value))
		}
		return remove, nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// IterateAI iterates over all AI metadata records whose keys match the given prefix
// (e.g., "ai:<model>:"). The callback receives the unmarshaled AIResultStruct for each record.
// If the callback returns remove=true and the iterator is in read-write mode, the key will be deleted.
func (r *BadgerIterator) IterateAI(prefix string, do func(key string, fData *AIResultStruct) (remove bool, err error)) error {
	return IterateJSON(r, prefix, do)
}

// IterateIndex iterates over all file index records whose keys match the given prefix
// (e.g., "file:"). The callback receives the unmarshaled FileData for each record.
// If the callback returns remove=true and the iterator is in read-write mode, the key will be deleted.
func (r *BadgerIterator) IterateIndex(prefix string, do func(fData *FileData) (remove bool, err error)) error {
	return IterateJSON(r, prefix, func(key string, entity *FileData) (remove bool, err error) {
		return do(entity)
	})
}

// Iterate performs a raw key-value scan across keys starting with prefix.
// The callback `do` is invoked for each item. If `do` returns remove=true, the key is collected
// and subsequently deleted in batched transactions (if readOnly is false).
func (r *BadgerIterator) Iterate(prefix string, do func(key, value []byte) (remove bool, err error)) error {
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
			k := item.KeyCopy(nil)
			if err := item.Value(func(val []byte) error {
				remove, err := do(k, val)
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
		txn := r.badgerDB.NewTransaction(true)
		defer func() {
			txn.Discard()
		}()
		r.logger.Info().Msgf("removing %d keys", len(removeKeys))
		for i, k := range removeKeys {
			r.logger.Info().Msgf("removing key '%s'", k)
			if err := txn.Delete(k); err != nil {
				r.logger.Error().Err(err).Msgf("cannot remove key '%s'", k)
			}
			if (i+1)%100 == 0 {
				r.logger.Info().Msgf("committing transaction with %d keys", i+1)
				if err := txn.Commit(); err != nil {
					r.logger.Error().Err(err).Msgf("cannot commit transaction")
					return errors.Wrapf(err, "cannot commit transaction")
				}
				txn = r.badgerDB.NewTransaction(true)
			}
		}
		if err := txn.Commit(); err != nil {
			r.logger.Error().Err(err).Msgf("cannot commit transaction")
			return errors.Wrapf(err, "cannot commit transaction")
		}
		r.logger.Info().Msgf("committed %d removed keys", len(removeKeys))
	}
	return nil
}
