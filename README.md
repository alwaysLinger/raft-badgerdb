## raft-badgerdb

A BadgerDB implementation of the [hashicorp/raft](https://github.com/hashicorp/raft) backend interfaces. This
implementation is based on the design of [raft-boltdb](https://github.com/hashicorp/raft-boltdb), but
uses [BadgerDB](https://github.com/dgraph-io/badger) as the underlying storage engine.

## Features

- Implements both `LogStore` and `StableStore` interfaces from hashicorp/raft
- Uses BadgerDB's key prefixes for efficient data organization and retrieval

## Usage
