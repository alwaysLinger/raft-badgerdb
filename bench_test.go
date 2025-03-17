package raftbadger

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkBoltStore_FirstIndex(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.FirstIndex(b, store)
}

func BenchmarkBoltStore_LastIndex(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.LastIndex(b, store)
}

func BenchmarkBoltStore_GetLog(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.GetLog(b, store)
}

func BenchmarkBoltStore_StoreLog(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.StoreLog(b, store)
}

func BenchmarkBoltStore_StoreLogs(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.StoreLogs(b, store)
}

func BenchmarkBoltStore_DeleteRange(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.DeleteRange(b, store)
}

func BenchmarkBoltStore_Set(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.Set(b, store)
}

func BenchmarkBoltStore_Get(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.Get(b, store)
}

func BenchmarkBoltStore_SetUint64(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.SetUint64(b, store)
}

func BenchmarkBoltStore_GetUint64(b *testing.B) {
	store := open(os.TempDir(), b)
	defer store.Close()
	defer os.Remove(store.dataPath)

	raftbench.GetUint64(b, store)
}
