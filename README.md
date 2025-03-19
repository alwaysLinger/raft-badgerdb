## raft-badgerdb

A BadgerDB implementation of the [hashicorp/raft](https://github.com/hashicorp/raft) backend interfaces. This
implementation is based on the design of [raft-boltdb](https://github.com/hashicorp/raft-boltdb), but
uses [BadgerDB](https://github.com/dgraph-io/badger) as the underlying storage engine.

## Features

- Implements both `LogStore` and `StableStore` interfaces from hashicorp/raft
- Uses BadgerDB's key prefixes for efficient data organization and retrieval

## Raft FSM

This project focuses on providing storage layer implementations for Raft, while a BadgerDB-based FSM (Finite State
Machine) implementation can be found in the [rbd-kv](https://github.com/alwaysLinger/rbd-kv) repository.

Reasons for separating the FSM implementation from the storage layer:

1. FSM implementations are typically more business-oriented, while storage layers are more generic
2. The [rbd-kv](https://github.com/alwaysLinger/rbd-kv) repository contains more feasibility analysis and design
   documentation
3. The FSM implementation from [rbd-kv](https://github.com/alwaysLinger/rbd-kv) can be trivially ported to this
   repository when needed

If you need a complete distributed KV storage system based on BadgerDB, just use them together 🌈
