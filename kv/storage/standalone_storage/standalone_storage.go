package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

// NewStandAloneStorage from config.
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{db: engine_util.CreateDB(conf.DBPath, false)}
}

// Start StandAloneStorage.
func (s *StandAloneStorage) Start() error {
	return nil
}

// Stop StandAloneStorage.
func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &standaloneReader{txn: s.db.NewTransaction(true)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value()); err != nil {
				return errors.Wrap(err, "Put error")
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key())); err != nil {
				return errors.Wrap(err, "Delete error")
			}
		}
	}

	return txn.Commit()
}

// standaloneReader is impl of `StorageReader`.
type standaloneReader struct {
	txn *badger.Txn
}

// GetCF.
func (s *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	return val, errors.WithStack(err)
}

// IterCF.
func (s *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

// Close.
func (s *standaloneReader) Close() {
	s.txn.Discard()
}
