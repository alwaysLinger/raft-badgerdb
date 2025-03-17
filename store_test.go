package raftbadger

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

func open(dataPath string, t testing.TB) *Store {
	store, err := NewStore(dataPath, nil, nil)
	if err != nil {
		t.Fatalf("err:%s\n", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestNewStore(t *testing.T) {
	dir := os.TempDir()
	store, err := NewStore(dir, nil, nil)
	if err != nil {
		t.Fatalf("err:%s", err)
	}
	defer func() {
		store.Close()
		os.Remove(dir)
	}()

	if store.dataPath != dir {
		t.Fatal("datapath dose not match dir")
	}
}

func TestStableStore_Set_Get(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q\n", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s\n", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s\n", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v\n", val)
	}
}

func TestStableStore_SetUint64_GetUint64(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

func TestLogStore_StoreLogs(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestLogStore_StoreLog(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestLogStore_FirstIndex(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestLogStore_LastIndex(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestLogStore_DeleteRange(t *testing.T) {
	store := open(os.TempDir(), t)
	store.db.DropAll()
	defer func() {
		store.db.DropAll()
		store.Close()
		os.Remove(store.dataPath)
	}()

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}

	if err := store.GetLog(3, new(raft.Log)); err != nil {
		t.Fatalf("log3 not found")
	}
}
