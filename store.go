package raftbadger

import (
	"errors"
	"log"
	"math"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var (
	// In BadgerDB, there is no concept of buckets. Instead, we use key prefixes
	// to manage and speed up lookups, which helps with data organization and retrieval.
	walPrefix  = []byte("l.")
	metaPrefix = []byte("m.")

	walPrefixLen  = len(walPrefix)
	metaPrefixLen = len(metaPrefix)

	ErrKeyNotFound = errors.New("not found")
)

// Store provides access to BadgerDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type Store struct {
	dataPath string

	db   *badger.DB
	opts *badger.Options

	copts      *CompactOptions
	gcTicker   *time.Ticker
	syncTicker *time.Ticker
}

type CompactOptions struct {
	forceInterval time.Duration
	ratio         float64
}

func NewStore(dataPath string, opts *badger.Options, copts *CompactOptions) (*Store, error) {
	if dataPath == "" {
		dataPath = os.TempDir()
	}

	if opts == nil {
		o := badger.DefaultOptions(dataPath).WithValueThreshold(8).WithDetectConflicts(false).WithMetricsEnabled(false).WithLogger(nil)
		opts = &o
	}

	db, err := badger.Open(*opts)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dataPath: dataPath,
		db:       db,
		opts:     opts,
	}

	if copts == nil {
		copts = defaultCompactionOptions()
	}
	s.copts = copts
	go s.runGC()
	go s.sync()

	return s, nil
}

func defaultCompactionOptions() *CompactOptions {
	return &CompactOptions{
		forceInterval: time.Hour * 2,
		ratio:         0.7,
	}
}

func (s *Store) runGC() {
	s.gcTicker = time.NewTicker(s.copts.forceInterval)
	for range s.gcTicker.C {
	again:
		err := s.db.RunValueLogGC(s.copts.ratio)
		if err == nil {
			goto again
		}
	}
}

func (s *Store) sync() {
	s.syncTicker = time.NewTicker(time.Minute * 30)
	for range s.syncTicker.C {
		if err := s.db.Sync(); err != nil {
			log.Printf("sync data error occurred: %v\n", err)
		}
	}
}

func (s *Store) Close() error {
	if s.gcTicker != nil {
		s.gcTicker.Stop()
	}
	if s.syncTicker != nil {
		s.syncTicker.Stop()
	}
	return s.db.Close()
}

func metaKey(key []byte) []byte {
	keyWithPrefix := make([]byte, metaPrefixLen+len(key))
	copy(keyWithPrefix, metaPrefix)
	copy(keyWithPrefix[metaPrefixLen:], key)
	return keyWithPrefix
}

func (s *Store) Set(key []byte, val []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(key), val)
	})
}

func (s *Store) Get(key []byte) ([]byte, error) {
	var (
		val []byte
		err error
	)

	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}

		val, err = item.ValueCopy(val)
		if err != nil {
			return err
		}
		return nil
	})

	return val, err
}

func (s *Store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

func (s *Store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(val), nil
}

func (s *Store) FirstIndex() (uint64, error) {
	return s.findFirstIndex(walPrefix, false)
}

func (s *Store) findFirstIndex(prefix []byte, reverse bool) (uint64, error) {
	var (
		rawIndex []byte
		err      error
	)

	err = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: walPrefix, Reverse: reverse})
		defer it.Close()
		it.Seek(prefix)

		if it.Valid() {
			rawIndex = it.Item().Key()
		}
		return nil
	})

	if err != nil || len(rawIndex) == 0 {
		return 0, err
	}

	return bytesToUint64(rawIndex[walPrefixLen:]), nil
}

func (s *Store) LastIndex() (uint64, error) {
	return s.findFirstIndex(append(walPrefix, uint64ToBytes(math.MaxUint64)...), true)
}

func (s *Store) GetLog(index uint64, log *raft.Log) error {
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(index))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return raft.ErrLogNotFound
			} else {
				return err
			}
		}

		return item.Value(func(val []byte) error {
			return decodeMsgPack(val, log)
		})
	})
}

// StoreLog stores a single log entry. In log replication process,
// logs are append-only and never modified, so there is no need to check
// for conflicts when storing a new log entry within BadgerDB.
// However, if a single log entry causes ErrTxnTooBig, using txn.Commit and retry
// won't help since the entry itself is too large. In this case, using Update is
// more appropriate as we can only return the error.
func (s *Store) StoreLog(log *raft.Log) error {
	val, err := encodeMsgPack(log, true)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(logKey(log.Index), val.Bytes())
	})
}

// StoreLogs stores multiple log entries
func (s *Store) StoreLogs(logs []*raft.Log) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for _, log := range logs {
		val, err := encodeMsgPack(log, true)
		if err != nil {
			return err
		}
		if err := wb.Set(logKey(log.Index), val.Bytes()); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func logKey(key uint64) []byte {
	keyWithPrefix := make([]byte, 8+walPrefixLen)
	copy(keyWithPrefix, walPrefix)
	copy(keyWithPrefix[walPrefixLen:], uint64ToBytes(key))
	return keyWithPrefix
}

// DeleteRange deletes multiple log entries. Since BadgerDB's batch operations expose errors
// without giving a chance to handle within the process, so manually manage the transaction here.
func (s *Store) DeleteRange(min, max uint64) error {
	txn := s.db.NewTransaction(true)
	it := txn.NewIterator(badger.IteratorOptions{Prefix: walPrefix})

	for it.Seek(logKey(min)); it.Valid(); it.Next() {
		key := make([]byte, 8+walPrefixLen)
		it.Item().KeyCopy(key)
		if bytesToUint64(key[walPrefixLen:]) > max {
			break
		}
		err := txn.Delete(key)
		if err != nil {
			if errors.Is(err, badger.ErrTxnTooBig) {
				it.Close()
				err = txn.Commit()
				if err != nil {
					return err
				}
				return s.DeleteRange(bytesToUint64(key[walPrefixLen:]), max)
			}
			return err
		}
	}

	it.Close()
	err := txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
