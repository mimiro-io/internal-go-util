package scheduler

import (
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
)

func Save[T any](db *badger.DB, collection string, id string, obj *T) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	indexBytes := []byte(collection)
	key := append(indexBytes, []byte("::"+id)...)
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, b)
	})
}

func FindAll[T any](db *badger.DB, collection string) ([]*T, error) {
	return FindSome[T](db, collection, "")
}

func FindSome[T any](db *badger.DB, collection string, prefix string) ([]*T, error) {
	result := make([]*T, 0)
	indexBytes := []byte(collection)
	startPrefix := append(indexBytes, []byte("::"+prefix)...)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(indexBytes); it.ValidForPrefix(startPrefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var p T
				err2 := json.Unmarshal(v, &p)
				if err2 != nil {
					return err2
				}
				result = append(result, &p)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

func FindOne[T any](db *badger.DB, collection string, id string) (*T, error) {
	indexBytes := []byte(collection)
	key := append(indexBytes, []byte("::"+id)...)

	var val []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}

		if err != nil {
			return err
		}

		val, err = item.ValueCopy(nil)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var p T
	err = json.Unmarshal(val, &p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func Delete(db *badger.DB, collection, id string) error {
	indexBytes := []byte(collection)
	key := append(indexBytes, []byte("::"+id)...)

	return db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
