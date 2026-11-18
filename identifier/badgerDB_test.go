package identifier

import (
	"encoding/json/v2"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

type sampleEntity struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestIterateJSON(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("failed to open in-memory badger: %v", err)
	}
	defer db.Close()

	// Seed items
	err = db.Update(func(txn *badger.Txn) error {
		items := map[string]sampleEntity{
			"entity:1": {Name: "First", Value: 10},
			"entity:2": {Name: "Second", Value: 20},
			"other:1":  {Name: "Other", Value: 30},
		}
		for k, v := range items {
			bytes, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(k), bytes); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed items: %v", err)
	}

	nopLogger := zerolog.Nop()
	iterator := &BadgerIterator{
		badgerDB: db,
		readOnly: true,
		logger:   &nopLogger,
	}

	collected := make(map[string]sampleEntity)
	err = IterateJSON[sampleEntity](iterator, "entity:", func(key string, entity *sampleEntity) (remove bool, err error) {
		collected[key] = *entity
		return false, nil
	})
	if err != nil {
		t.Fatalf("IterateJSON failed: %v", err)
	}

	if len(collected) != 2 {
		t.Errorf("expected 2 entities matching prefix 'entity:', got %d", len(collected))
	}
	if collected["entity:1"].Name != "First" || collected["entity:1"].Value != 10 {
		t.Errorf("unexpected data for entity:1: %+v", collected["entity:1"])
	}
	if collected["entity:2"].Name != "Second" || collected["entity:2"].Value != 20 {
		t.Errorf("unexpected data for entity:2: %+v", collected["entity:2"])
	}
}
