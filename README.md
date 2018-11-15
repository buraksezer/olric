# Olric

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/buraksezer/olric) [![Coverage Status](https://coveralls.io/repos/github/buraksezer/olric/badge.svg?branch=master)](https://coveralls.io/github/buraksezer/olric?branch=master) [![Build Status](https://travis-ci.org/buraksezer/olric.svg?branch=master)](https://travis-ci.org/buraksezer/olric) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/olric)](https://goreportcard.com/report/github.com/buraksezer/olric) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Distributed and in-memory key/value database that persists on disk. It can be used both as an embedded Go library and as 
a language-independent service. Built with [Go](https://golang.org).
## WIP

This project is a work in progress. The implementation is incomplete. The documentation may be inaccurate.

## Table of Contents

* [Features](#features)
* [Planned Features](#planned-features)
* [Installing](#installing)
* [Usage](#usage)
  * [Put](#put)
  * [PutEx](#putex)
  * [Get](#get)
  * [Delete](#delete)
  * [LockWithTimeout](#lockwithtimeout)
  * [Unlock](#unlock)
  * [Destroy](#destroy)
  * [Atomic Operations](#atomic-operations)
    * [Incr](#incr)
    * [Decr](#decr)
    * [GetPut](#getput)
* [Persistence](#persistence)
* [Golang Client](#golang-client)
* [Standalone Server](#standalone-server)
* [Command Line Interface](#command-line-interface)
* [Operation Modes](#operation-modes)
  * [Embedded member](#embedded-member)
  * [Client plus member](#client-plus-member)
* [Configuration](#configuration)
* [Architecture](#architecture)
  * [Overview](#overview)
  * [Consistency and Replication Model](#consistency-and-replication-model)
  * [Eviction](#eviction)
  * [Lock Implementation](#lock-implementation)
* [Sample Code](#sample-code)
* [To-Do](#to-do)
* [Caveats](#caveats)

## Features

* Designed to share some transient, approximate, fast-changing data between servers,
* Accepts arbitrary types as value,
* In-memory with optional persistence,
* Implements a fast and simple binary protocol,
* Embeddable but can be used as a language-independent service with olricd,
* Stores values in off-heap memory, which is memory within the runtime that is not subject to Go garbage collection.
* Supports atomic operations,
* Provides a single-node lock implementation which can be used for non-critical purposes,
* Time-To-Live(TTL) eviction policy,
* Highly available,
* Horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP solution,
* Distributes load fairly among cluster members with a [consistent hash function](https://github.com/buraksezer/consistent),
* Supports replication by default(with sync and async options),
* Thread-safe by default,
* Provides a command-line-interface to access the cluster directly from the terminal,
* Very simple package API,
* Offers a built-in Go client,
* Gob, JSON and MessagePack are supportted by default as serialization format,
* Simplicity as a key concept with a small set of features.

## Planned Features

* Anti-entropy system to repair inconsistencies in DMaps,
* LRU eviction policy,
* Publish/Subscribe for messaging,
* Eviction listeners by using Pub/Sub,
* Memcached interface,
* Python client.

We may implement different data structures such as list, queue or bitmap in Olric. It's highly depends on attention of the Golang community.

## Installing

With a correctly configured Golang environment:

```
go get -u github.com/buraksezer/olric
```

## Usage

Olric is designed to work efficiently with the minimum amount of configuration. So the default configuration should be enough for experimenting:

```go
db, err := olric.New(nil)
```

This creates an Olric object without running any server at background. In order to run Olric, you need to call **Start** method.

```go
err := db.Start()
```

When you call **Start** method, your process joins the cluster and will be responsible for some parts of the data. This call blocks
indefinitely. So you may need to run it in a goroutine. Of course, this is just a single-node instance, because you didn't give any
configuration.

Create a **DMap** object to access the cluster:

```go
dm := db.NewDMap("my-dmap")
```

DMap object has *Put*, *PutEx*, *Get*, *Delete*, *LockWithTimeout*, *Unlock* and *Destroy* methods to access and modify data in Olric. 
We may add more methods for finer control but first, I'm willing to stabilize this set of features.

When you want to leave the cluster, just need to call **Shutdown** method:

```go
err := db.Shutdown(context.Background())
```

This will stop background tasks, then call **Shutdown** methods of HTTP server and memberlist, respectively.

### Put

Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.

```go
err := dm.Put("my-key", "my-value")
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after
Put returns but not before.

### PutEx

Put sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.

```go
err := dm.PutEx("my-key", "my-value", time.Second)
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after PutEx 
returns but not before.

### Get

Get gets the value for the given key. It returns `ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

```go
value, err := dm.Get("my-key")
```

It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.

### Delete

Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.

```go
err := dm.Delete("my-key")
```

It is safe to modify the contents of the argument after Delete returns.

### LockWithTimeout

LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time, it automatically releases the
lock. Acquired lock is only for the key in this map. Please note that, before setting a lock for a key, you should set the key with **Put** method. 
Otherwise it returns `ErrKeyNotFound` error.

```go
err := dm.LockWithTimeout("my-key", time.Second)
```

It returns immediately if it acquires the lock for the given key. Otherwise, it waits until timeout. The timeout is determined by `http.Client`
which can be configured via `Config` structure.

**You should know that the locks are approximate, and only to be used for non-critical purposes.**

Please take a look at [Lock Implementation](#lock-implementation) section for implementation details.

### Unlock

Unlock releases an acquired lock for the given key. It returns `ErrNoSuchLock` if there is no lock for the given key.

```go
err := dm.Unlock("my-key")
```

### Destroy

Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps. So if you call Put/PutEx and Destroy
methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.

```go
err := dm.Destroy()
```

## Configuration

[memberlist configuration](https://godoc.org/github.com/hashicorp/memberlist#Config) can be tricky and and the default configuration set should be tuned for your environment. A detailed deployment and configuration guide will be prepared before stable release.

Please take a look at [Config section at godoc.org](https://godoc.org/github.com/buraksezer/olric#Config)

Here is a sample configuration for a cluster with two hosts:

```go
m1, _ := olric.NewMemberlistConfig("local")
m1.BindAddr = "127.0.0.1"
m1.BindPort = 5555
c1 := &olric.Config{
	Name:          "127.0.0.1:3535", // Unique in the cluster and used by HTTP server.
	Peers:         []string{"127.0.0.1:5656"},
	MemberlistCfg: m1,
}

m2, _ := olric.NewMemberlistConfig("local")
m2.BindAddr = "127.0.0.1"
m2.BindPort = 5656
c2 := &olric.Config{
	Name:          "127.0.0.1:3636",
	Peers:         []string{"127.0.0.1:5555"},
	MemberlistCfg: m2,
}

db1, err := olric.New(c1)
// Check error

db2, err := olric.New(c2)
// Check error

// Call Start method for db1 and db2 in a seperate goroutine.
```

## Architecture

### Overview

Olric uses:

* [hashicorp/memberlist](https://github.com/hashicorp/memberlist) for cluster membership and failure detection,
* [buraksezer/consistent](https://github.com/buraksezer/consistent) for consistent hashing and load balancing,
* [net/http](https://golang.org/pkg/net/http/) as transport layer,
* [encoding/gob](https://golang.org/pkg/encoding/gob/) for serialization.

Olric distributes data among partitions. Every partition is owned by a cluster member and may has one or more backup for redundancy. 
When you read or write a map entry, you transparently talk to the partition owner. Each request hits the most up-to-date version of a
particular data entry in a stable cluster.

In order to find the partition which the key belongs to, Olric hashes the key and mod it with the number of partitions:

```
partID = MOD(hash result, partition count)
```

The partitions are distributed among cluster members by using a consistent hashing algorithm. In order to get details, please see
[buraksezer/consistent](https://github.com/buraksezer/consistent). The backup owners are also calculated by the same package.

When a new cluster is created, one of the instances elected as the **cluster coordinator**. It manages the partition table: 

* When a node joins or leaves, it distributes the partitions and their backups among the members again,
* Removes empty owners from the partition owners list,
* Pushes the new partition table to all the members,
* Pushes the the partition table to the cluster periodically.

Members propagates their birthdate(Unix timestamp in nanoseconds) to the cluster. The coordinator is the oldest member in the cluster.
If the coordinator leaves the cluster, the second oldest member elected as the coordinator.

Olric has a component called **fsck** which is responsible for keeping underlying data structures consistent:

* Works on every node,
* When a node joins or leaves, the cluster coordinator pushes the new partition table. Then, fsck goroutine runs immediately and moves the partitions and backups to their new hosts,
* Merges fragmented partitions,
* Runs at background periodically and repairs partitions i.e. creates new backups if required.

Partitions have a concept called **owners list**. When a node joins or leaves the cluster, a new primary owner may be assigned by the 
coordinator. At any time, a partition may has one or more partition owner. If a partition has two or more owner, this is called **fragmented partition**. The last added owner is called **primary owner**. Write operation is only done by the primary owner. The previous owners are only
used for read and delete.

When you read a key, the primary owner tries to find the key on itself, first. Then, queries the previous owners and backups, respectively. 
Delete operation works with the same way.

The data(distributed map objects) in the fragmented partition is moved slowly to the primary owner by **fsck** goroutine. Until the move is done,
the data remains available on the previous owners. DMap methods use this list to query data on the cluster.

*Please note that, multiple partition owner is an undesirable situation and the fsck component is designed to fix that in a short time.*

Olric uses HTTP as transport layer. It's suitable to transfer small messages between servers. **[HTTP/2](https://hpbn.co/http2/) is highly
recommended for production use** because it uses a single TCP socket to deliver multiple requests and responses in parallel.

When you call **Start** method of Olric, it starts an HTTP server at background which can be configured by the user via **Config** struct.

### Consistency and Replication Model

[Olric is an AP product](https://en.wikipedia.org/wiki/CAP_theorem), which employs the combination of primary-copy and [optimistic replication](https://en.wikipedia.org/wiki/Optimistic_replication) techniques. With optimistic replication, when the partition owner receives a write or delete 
operation for a key, applies it locally, and propagates it to backup owners.

This technique enables Olric clusters to offer high throughput. However, due to temporary situations in the system, such as network
failure, backup owners can miss some updates and diverge from the primary owner. If a partition owner crashes while there is an
inconsistency between itself and the backups, strong consistency of the data can be lost.

Two types of backup replication are available: **sync** and **async**. Both types are still implementations of the optimistic replication
model.

* **sync**: Blocks until write/delete operation is applied by backup owners.
* **async**: Just fire & forget.

An anti-entropy system has been planned to deal with inconsistencies in DMaps.

### Eviction

Olric only implements TTL eviction policy. It shares the same algorithm with [Redis](https://redis.io/commands/expire#appendix-redis-expires):

> Periodically Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are deleted from the keyspace.
>
> Specifically this is what Redis does 10 times per second:
>
> * Test 20 random keys from the set of keys with an associated expire.
> * Delete all the keys found expired.
> * If more than 25% of keys were expired, start again from step 1.
>
> This is a trivial probabilistic algorithm, basically the assumption is that our sample is representative of the whole key space, and we continue to expire until the percentage of keys that are likely to be expired is under 25%

When a client tries to access a key, Olric returns `ErrKeyNotFound` if the key is found to be timed out. A background task evicts keys with the algorithm described above.

LRU eviction policy implementation has been planned.

### Lock Implementation

DMap implementation is already thread-safe to meet your thread safety requirements. When you want to have more control on the
concurrency, you can use LockWithTimeout method. It's slightly modified version of [Moby's(formerly Docker) locker package](https://github.com/moby/moby/tree/master/pkg/locker). It utilizes `sync.Mutex`. Take a look at the code for details.

Please note that the lock implementation has no backup. So if the node, which the lock belongs to, crashed, the acquired lock is dropped.

**I recommend the lock implementation to be used for efficiency purposes in general, instead of correctness.**

## Client

Olric is mainly designed to be used as an embedded [DHT](https://en.wikipedia.org/wiki/Distributed_hash_table). So if you are running long-lived servers,
Olric is pretty suitable to share some transient, approximate, fast-changing data between them. What if you want to access the cluster in a short-lived
process? Fortunately, Olric has a simple HTTP API which can be used to access the cluster within any environment. It will be documented soon.

A Golang client is already prepared to access and modify DMaps from outside. [Here is the documentation](https://godoc.org/github.com/buraksezer/olric/client).

## Sample Code

The following snipped can be run on your computer directly. It's a single-node setup, of course:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/buraksezer/olric"
)

type customType struct {
	Field1 string
	Field2 uint64
}

func main() {
	// This creates a single-node Olric cluster. It's good enough for experimenting.
	db, err := olric.New(nil)
	if err != nil {
		log.Fatalf("Failed to create Olric object: %v", err)
	}

	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("Failed to call Start: %v", err)
		}
	}()

	// Put 10 items into the DMap object.
	dm := db.NewDMap("bucket-of-arbitrary-items")
	for i := 0; i < 10; i++ {
		c := customType{}
		c.Field1 = fmt.Sprintf("num: %d", i)
		c.Field2 = uint64(i)
		err = dm.Put(strconv.Itoa(i), c)
		if err != nil {
			log.Printf("Put call failed: %v", err)
		}
	}

	// Read them again.
	for i := 0; i < 10; i++ {
		val, err := dm.Get(strconv.Itoa(i))
		if err != nil {
			log.Printf("Get call failed: %v", err)
		}
		fmt.Println(val, reflect.TypeOf(val))
	}

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}
```

## To-Do

* Document the code,
* Some parts of FSCK implementation is missing: It currently doesn't repair failed backups,
* Design & write benchmarks,
* Document the binary protocol,
* Build a website for Olric and create extensive documentation.

## Caveats

Olric uses Golang's built-in map. It's known that the built-in map has problems with the GC:

* [Go GC and maps with pointers](https://www.komu.engineer/blogs/go-gc-maps)
* [GC is bad but you shouldn’t feel bad](https://syslog.ravelin.com/gc-is-bad-and-you-should-feel-bad-e9bdd9324f0)

Olric already uses `map[uint64][]byte` as underlying data structure. It should work fine for most of the cases.

I have implemented an off-heap hash table with [mmap](http://man7.org/linux/man-pages/man2/mmap.2.html). We may add an option to use it in the future but 
my implementation needs too much effort to be used in production.

## Contributions

Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

## License

The Apache License, Version 2.0 - see LICENSE for more details.

## About the name

The inner voice of Turgut Özben who is the main character of [Oğuz Atay's masterpiece -The Disconnected-](https://www.bariscayli.com/single-post/2016/12/20/Tutunamayanlar---The-Disconnected).
