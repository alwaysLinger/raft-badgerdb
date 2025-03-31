package raftbadger

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkBadgerStore_FirstIndex(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.FirstIndex(b, store)
}

func BenchmarkBadgerStore_LastIndex(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.LastIndex(b, store)
}

func BenchmarkBadgerStore_GetLog(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.GetLog(b, store)
}

func BenchmarkBadgerStore_StoreLog(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.StoreLog(b, store)
}

func BenchmarkBadgerStore_StoreLogs(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.StoreLogs(b, store)
}

func BenchmarkBadgerStore_DeleteRange(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.DeleteRange(b, store)
}

func BenchmarkBadgerStore_Set(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.Set(b, store)
}

func BenchmarkBadgerStore_Get(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.Get(b, store)
}

func BenchmarkBadgerStore_SetUint64(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.SetUint64(b, store)
}

func BenchmarkBadgerStore_GetUint64(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.GetUint64(b, store)
}
