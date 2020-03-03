# Olric [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Olric%3A+Distributed+and+in-memory+key%2Fvalue+database.+It+can+be+used+both+as+an+embedded+Go+library+and+as+a+language-independent+service.+&url=https://github.com/buraksezer/olric&hashtags=golang,distributed,database)

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/buraksezer/olric) [![Coverage Status](https://coveralls.io/repos/github/buraksezer/olric/badge.svg?branch=master)](https://coveralls.io/github/buraksezer/olric?branch=master) [![Build Status](https://travis-ci.org/buraksezer/olric.svg?branch=master)](https://travis-ci.org/buraksezer/olric) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/olric)](https://goreportcard.com/report/github.com/buraksezer/olric) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Distributed cache and in-memory key/value data store. It can be used both as an embedded Go library and as a language-independent service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across a cluster of computers.

## At a glance

* Designed to share some transient, approximate, fast-changing data between servers,
* Embeddable but can be used as a language-independent service with *olricd*,
* Supports different eviction algorithms,
* Fast binary protocol,
* Highly available and horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Supports replication by default(with sync and async options),
* Quorum-based voting for replica control(Read/Write quorums),
* Supports atomic operations,
* Supports distributed queries on keys,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode).

See [Sample Code](https://github.com/buraksezer/olric#sample-code) section for a quick experimentation.

## Possible Use Cases

With this feature set, Olric is suitable to use as a distributed cache. But it also provides data replication, failure detection 
and simple anti-entropy services. So it can be used as an ordinary key/value data store to scale your cloud application.

## Project Status

Olric is in early stages of development. The package API and client protocol may change without notification. 

## Table of Contents

* [Features](#features)
* [Planned Features](#planned-features)
* [Installing](#installing)
  * [Try with Docker](#try-with-docker)
* [Operation Modes](#operation-modes)
  * [Embedded Member](#embedded-member)
  * [Client-Server](#client-server)
* [Tooling](#tooling)
  * [olricd](#olricd)
  * [olric-cli](#olric-cli)
  * [olric-stats](#olric-stats)
  * [olric-load](#olric-load)
* [Performance](#performance)
* [Usage](#usage)
  * [Put](#put)
  * [PutIf](#putif)
  * [PutEx](#putex)
  * [PutIfEx](#putifex)
  * [Get](#get)
  * [Expire](#expire)
  * [Delete](#delete)
  * [LockWithTimeout](#lockwithtimeout)
  * [Lock](#lock)
  * [Unlock](#unlock)
  * [Destroy](#destroy)
  * [Stats](#stats)
  * [Ping](#ping)
  * [Query](#query)
    * [Cursor](#cursor)
      * [Range](#range)
      * [Close](#close)
  * [Atomic Operations](#atomic-operations)
    * [Incr](#incr)
    * [Decr](#decr)
    * [GetPut](#getput)
  * [Pipelining](#pipelining)
* [Serialization](#serialization)
* [Golang Client](#golang-client)
* [Configuration](#configuration)
* [Architecture](#architecture)
  * [Overview](#overview)
  * [Consistency and Replication Model](#consistency-and-replication-model)
    * [Last-write-wins conflict resolution](#last-write-wins-conflict-resolution)
    * [PACELC Theorem](#pacelc-theorem)
    * [Read-Repair on DMaps](#read-repair-on-dmaps)
    * [Quorum-based Replica Control](#quorum-based-replica-control)
    * [Simple Split-Brain Protection](#simple-split-brain-protection)
  * [Eviction](#eviction)
    * [Expire with TTL](#expire-with-ttl)
    * [Expire with MaxIdleDuration](#expire-with-maxidleduration)
    * [Expire with LRU](#expire-with-lru)
  * [Lock Implementation](#lock-implementation)
  * [Storage Engine](#storage-engine)
* [Sample Code](#sample-code)
* [Contributions](#contributions)
* [License](#license)
* [About the name](#about-the-name)


## Features

* Designed to share some transient, approximate, fast-changing data between servers,
* Accepts arbitrary types as value,
* Only in-memory,
* Implements a fast and simple binary protocol,
* Embeddable but can be used as a language-independent service with olricd,
* GC-friendly storage engine,
* O(1) running time for lookups,
* Supports atomic operations,
* Provides a lock implementation which can be used for non-critical purposes,
* Different eviction policies: LRU, MaxIdleDuration and Time-To-Live(TTL),
* Highly available,
* Horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Distributes load fairly among cluster members with a [consistent hash function](https://github.com/buraksezer/consistent),
* Supports replication by default(with sync and async options),
* Quorum-based voting for replica control,
* Thread-safe by default,
* Supports distributed queries on keys,
* Provides a command-line-interface to access the cluster directly from the terminal,
* Supports different serialization formats. Gob, JSON and MessagePack are supported out of the box,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode).

See [Architecture](#architecture) section to see details.

## Planned Features

* Distributed queries over keys and values,
* Database backend for persistence,
* Anti-entropy system to repair inconsistencies in DMaps,
* Publish/Subscribe for messaging,
* Eviction listeners by using Publish/Subscribe,
* Memcached interface,
* Client implementations for different languages: Java, Python and JavaScript,
* REST API.

## Installing

With a correctly configured Golang environment:

```
go get -u github.com/buraksezer/olric
```

Then, install olricd and its siblings:

```
go install -v ./cmd/*
```

Now you should access **olricd**, **olric-stats**, **olric-cli** and **olric-load** on your path. You can just run olricd
to start experimenting: 

```
olricd -c cmd/olricd/olricd.yaml
```

See [Configuration](#configuration) section to setup your cluster properly.

### Try with Docker

This repository includes a Dockerfile. So you can build and run ```olricd``` in a Docker container. Use the following commands 
respectively in the project folder:

```
docker build -t olricd .
```

This command will build ```olricd``` in a container. Then you can start ```olricd``` by using the following command:

```
docker run -p 3320:3320 olricd
```

Your programs can use ```3320``` port to interact with ```olricd```. 

## Operation Modes

Olric has two different operation modes. 

### Embedded Member

In Embedded Member Mode, members include both the application and Olric data and services. The advantage of the Embedded 
Member Mode is having a low-latency data access and locality.

### Client-Server

In the Client-Server deployment, Olric data and services are centralized in one or more server members and they are 
accessed by the application through clients. You can have a cluster of server members that can be independently created 
and scaled. Your clients communicate with these members to reach to Olric data and services on them.

Client-Server deployment has advantages including more predictable and reliable performance, easier identification 
of problem causes and, most importantly, better scalability. When you need to scale in this deployment type, just add more 
Olric server members. You can address client and server scalability concerns separately. 

See [olricd](#olricd) section to get started.

Currently we only have the official Golang client. A possible Python implementation is on the way. After stabilizing the
Olric Binary Protocol, the others may appear quickly.

## Tooling

Olric comes with some useful tools to interact with the cluster. 

### olricd

With olricd, you can create an Olric cluster with a few commands. Let's create a cluster with the following:

```
olricd -c <YOUR_CONFIG_FILE_PATH>
```

olricd also supports `OLRICD_CONFIG` environment variable to set configuration. Just like that: 

```
OLRICD_CONFIG=<YOUR_CONFIG_FILE_PATH> olricd
```

Olric nodes discovers the others automatically. You just need to maintain an accurate list of peers as much as possible.
Currently we only support a static peer list in the configuration file. The other methods will be implemented in the future. 
You should know that a single alive peer in the list is enough to discover the whole cluster. 

You can find a sample configuration file under `cmd/olricd/olricd.yaml`. 

### olric-cli

olric-cli is the Olric command line interface, a simple program that allows to send commands to Olric, and read the replies 
sent by the server, directly from the terminal.

olric-cli has an interactive (REPL) mode just like `redis-cli`:

```
olric-cli
[127.0.0.1:3320] >> use mydmap
use mydmap
[127.0.0.1:3320] >> get mykey
myvalue
[127.0.0.1:3320] >>
```

The interactive mode also keeps command history. 

It's possible to send protocol commands as command line arguments:

```
olric-cli -d mydmap -c "put mykey myvalue"
```

Then, retrieve the key:

```
olric-cli -d mydmap -c "get mykey"
```

It'll print `myvalue`.


In order to get more details about the options, call `olric-cli -h` in your shell.

### olric-stats 

olric-stats calls `Stats` command on a cluster member and prints the result. The returned data from the member includes the Go runtime 
metrics and statistics from hosted primary and backup partitions. 

Statistics about a partition:

```
olric-stats -p 69
PartID: 69
  Owner: olric.node:3320
  Previous Owners: not found
  Backups: not found
  DMap count: 1
  DMaps:
    Name: olric-load-test
    Length: 1374
    Allocated: 1048576
    Inuse: 47946
    Garbage: 0
```

In order to get detailed statistics about the Go runtime, you should call `olric-stats -a <ADDRESS> -r`.

Without giving a partition number, it will print everything about the cluster and hosted primary/backup partitions. 
In order to get more details about the command, call `olric-stats -h`.

### olric-load

olric-load simulates running commands done by N clients at the same time sending M total queries. It measures response time. 

```
olric-load -c put -s msgpack -k 100000
### STATS FOR COMMAND: PUT ###
Serializer is msgpack
100000 requests completed in 1.209334678s
50 parallel clients

  93%  <=  1 milliseconds
   5%  <=  2 milliseconds
```

In order to get more details about the command, call `olric-load -h`.

## Usage

Olric is designed to work efficiently with the minimum amount of configuration. So the default configuration should be enough for experimenting:

```go
db, err := olric.New(config.New())
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
dm, err := db.NewDMap("my-dmap")
```

DMap object has *Put*, *PutEx*, *PutIf*, *PutIfEx*, *Get*, *Delete*, *Expire*, *LockWithTimeout* and *Destroy* methods to access 
and modify data in Olric. We may add more methods for finer control but first, I'm willing to stabilize this set of features.

When you want to leave the cluster, just need to call **Shutdown** method:

```go
err := db.Shutdown(context.Background())
```

This will stop background tasks and servers. Finally purges in-memory data and quits.

***Please note that this section aims to document DMap API in embedded member mode.*** If you prefer to use Olric in 
Client-Server mode, please jump to [Golang Client](#golang-client) section. 
 
### Put

Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.

```go
err := dm.Put("my-key", "my-value")
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after
Put returns but not before.

### PutIf

PutIf sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.

```go
err := dm.PutIf("my-key", "my-value", flags)
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after
PutIf returns but not before.

Flag argument currently has two different options:

* **IfNotFound**: Only set the key if it does not already exist. It returns `ErrFound` if the key already exist.

* **IfFound**: Only set the key if it already exist.It returns `ErrKeyNotFound` if the key does not exist.

Sample use:

```go
err := dm.PutIfEx("my-key", "my-value", time.Second, IfNotFound)
```

### PutEx

PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.

```go
err := dm.PutEx("my-key", "my-value", time.Second)
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after PutEx 
returns but not before.

### PutIfEx

PutIfEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.

```go
err := dm.PutIfEx("my-key", "my-value", time.Second, flags)
```

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after PutIfEx 
returns but not before.


Flag argument currently has two different options:

* **IfNotFound**: Only set the key if it does not already exist. It returns `ErrFound` if the key already exist.

* **IfFound**: Only set the key if it already exist.It returns `ErrKeyNotFound` if the key does not exist.

Sample use:

```go
err := dm.PutIfEx("my-key", "my-value", time.Second, IfNotFound)
```

### Get

Get gets the value for the given key. It returns `ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

```go
value, err := dm.Get("my-key")
```

It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.

### Expire

Expire updates the expiry for the given key. It returns `ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

```go
err := dm.Expire("my-key", time.Second)
```

The key has to be `string`. The second parameter is `time.Duration`.

### Delete

Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.

```go
err := dm.Delete("my-key")
```

It is safe to modify the contents of the argument after Delete returns.

### LockWithTimeout

LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time, it automatically releases the
lock. Acquired lock is only for the key in this DMap.

```go
ctx, err := dm.LockWithTimeout("lock.foo", time.Millisecond, time.Second)
```

It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline. You should keep `LockContext` (as ctx) 
value to call **Unlock** method to release the lock.

Creating a seperated DMap to keep locks may be a good idea.

**You should know that the locks are approximate, and only to be used for non-critical purposes.**

Please take a look at [Lock Implementation](#lock-implementation) section for implementation details.

### Lock
Lock sets a lock for the given key. Acquired lock is only for the key in this DMap.

```go
ctx, err := dm.Lock("lock.foo", time.Second)
```

It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline. You should keep `LockContext` (as ctx) 
value to call **Unlock** method to release the lock.

**You should know that the locks are approximate, and only to be used for non-critical purposes.**

### Unlock

Unlock releases an acquired lock for the given key. It returns `ErrNoSuchLock` if there is no lock for the given key.

```go
err := ctx.Unlock()
```

### Destroy

Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps. So if you call Put/PutEx and Destroy
methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.

```go
err := dm.Destroy()
```

### Stats

Stats exposes some useful metrics to monitor an Olric node. It includes memory allocation metrics from partitions and the Go runtime metrics.

```go
data, err := db.Stats()
```

See `stats/stats.go` for detailed info about the metrics.

### Ping 

Ping sends a dummy protocol messsage to the given host. This is useful to measure RTT between hosts. It also can be used as aliveness check.

```go
err := db.Ping()
```
### Query

Query runs a distributed query on a DMap instance. Olric supports a very simple query DSL and now, it only scans keys. 
The query DSL has very few keywords:

* **$onKey**: Runs the given query on keys or manages options on keys for a given query.
* **$onValue**: Runs the given query on values or manages options on values for a given query.
* **$options**: Useful to modify data returned from a query

Keywords for $options:

* **$ignore**: Ignores a value.

A distributed query looks like the following:

```go
  query.M{
	  "$onKey": query.M{
		  "$regexMatch": "^even:",
		  "$options": query.M{
			  "$onValue": query.M{
				  "$ignore": true,
			  },
		  },
	  },
  }
```

This query finds the keys starts with *even:*, drops the values and returns only keys. If you also want to retrieve the values, 
just remove the **$options** directive:

```go
  query.M{
	  "$onKey": query.M{
		  "$regexMatch": "^even:",
	  },
  }
```

In order to iterate over all the keys:

```go
  query.M{
	  "$onKey": query.M{
		  "$regexMatch": "",
	  },
  }
```

This is how you call a distributed query over the cluster:

```go
c, err := dm.Query(query.M{"$onKey": query.M{"$regexMatch": "",}})
```

Query function returns a cursor which has `Range` and `Close` methods. Please take look at the `Range` function for further info. 

[Here is a working query example.](https://gist.github.com/buraksezer/045b7ec09463e38b383d0413ad9bcc57)

### Cursor

Cursor implements distributed queries in Olric. It has two methods: `Range` and `Close`

#### Range

Range calls `f` sequentially for each key and value yielded from the cursor. If f returns `false`, range stops the iteration.

```go
err := c.Range(func(key string, value interface{}) bool {
		fmt.Printf("KEY: %s, VALUE: %v\n", key, value)
		return true
})
```

#### Close

Close cancels the underlying context and background goroutines stops running. It's a good idea that defer `Close` after getting
a `Cursor`. By this way, you can ensure that there is no dangling goroutine after your distributed query execution is stopped. 

```go
c.Close()
```

## Atomic Operations

Normally, write operations in Olric is performed by the partition owners. However, atomic operations are guarded by a fine-grained lock 
implementation which can be found under `internal/locker`. 

You should know that Olric is an AP product. So Olric may return inconsistent results in the case of network partitioning. 

`internal/locker` is provided by the [Docker](https://github.com/moby/moby).

### Incr

Incr atomically increments key by delta. The return value is the new value after being incremented or an error.

```go
nr, err := dm.Incr("atomic-key", 3)
```

The returned value is `int`.

### Decr

Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.

```go
nr, err := dm.Decr("atomic-key", 1)
```

The returned value is `int`.


### GetPut

GetPut atomically sets key to value and returns the old value stored at key.

```go
value, err := dm.GetPut("atomic-key", someType{})
```

The returned value is an arbitrary type.

### Pipelining
Olric Binary Protocol(OBP) supports pipelining. All protocol commands can be pushed to a remote Olric server through a pipeline in a single write call. 
A sample use looks like the following:
```go
// Create an ordinary Olric client, not Olric node!
// ...
// Create a new pipe and call on it whatever you want.
pipe := client.NewPipeline()
for i := 0; i < 10; i++ {
    key := "key-" + strconv.Itoa(i)
    err := pipe.Put("mydmap", key, i)
    if err != nil {
        fmt.Println("returned an error: ", err)
    }
}

for i := 0; i < 10; i++ {
    key := "key-" + strconv.Itoa(i)
    err := pipe.Get("mydmap", key)
    if err != nil {
        fmt.Println("returned an error: ", err)
    }
}

// Flush messages to the server.
responses, err := pipe.Flush()
if err != nil {
    fmt.Println("returned an error: ", err)
}

// Read responses from the pipeline.
for _, resp := range responses {
    if resp.Operation() == "Get" {
        val, err := resp.Get()
        if err != nil {
            fmt.Println("returned an error: ", err)
        }
        fmt.Println("Get response: ", val)
    }
}
```

There is no hard-limit on message count in a pipeline. You should set a convenient `KeepAlive` for large pipelines. 
Otherwise you can get a timeout error.

The `Flush` method returns errors along with success messages. Furhermore, you need to know the command order to match responses with requests.

## Golang Client
This repo contains the official Golang client for Olric. It implements Olric Binary Protocol(OBP). With this client,
you can access to Olric clusters in your Golang programs. In order to create a client instance:
```go
var clientConfig = &client.Config{
    Addrs:       []string{"localhost:3320"},
    DialTimeout: 10 * time.Second,
    KeepAlive:   10 * time.Second,
    MaxConn:     100,
}

client, err := client.New(clientConfig)
if err != nil {
    return err
}

dm := client.NewDMap("foobar")
err := dm.Put("key", "value")
// Handle this error
```

This implementation supports TCP connection pooling. So it recycles the opened TCP connections to avoid wasting resources. 
The requests distributes among available TCP connections using an algorithm called `round-robin`. In order to see detailed list of
configuration parameters, see [Olric documentation on GoDoc.org](https://godoc.org/github.com/buraksezer/olric).

## Configuration

[memberlist configuration](https://godoc.org/github.com/hashicorp/memberlist#Config) can be tricky and and the default configuration set should be tuned for your environment. A detailed deployment and configuration guide will be prepared before stable release.

Please take a look at [Config section at godoc.org](https://godoc.org/github.com/buraksezer/olric#Config)

Here is a sample configuration for a cluster with two hosts:

```go
m1, _ := olric.NewMemberlistConfig("local")
m1.BindAddr = "127.0.0.1"
m1.BindPort = 5555
c1 := &olric.Config{
	Name:          "127.0.0.1:3535", // Unique in the cluster and used by TCP server.
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
* [Golang's TCP implementation](https://golang.org/pkg/net/#TCPConn) as transport layer,
* Different alternatives for serialization:
    * [encoding/gob](https://golang.org/pkg/encoding/gob/),
    * [encoding/json](https://golang.org/pkg/encoding/json/), 
    * [vmihailenco/msgpack](https://github.com/vmihailenco/msgpack).

Olric distributes data among partitions. Every partition is owned by a cluster member and may have one or more backups for redundancy. 
When you read or write a DMap entry, you transparently talk to the partition owner. Each request hits the most up-to-date version of a
particular data entry in a stable cluster.

In order to find the partition which the key belongs to, Olric hashes the key and mod it with the number of partitions:

```
partID = MOD(hash result, partition count)
```

The partitions are distributed among cluster members by using a consistent hashing algorithm. In order to get details, please see
[buraksezer/consistent](https://github.com/buraksezer/consistent). The backup owners are also calculated by the same package.

When a new cluster is created, one of the instances is elected as the **cluster coordinator**. It manages the partition table: 

* When a node joins or leaves, it distributes the partitions and their backups among the members again,
* Removes empty owners from the partition owners list,
* Pushes the new partition table to all the members,
* Pushes the the partition table to the cluster periodically.

Members propagates their birthdate(Unix timestamp in nanoseconds) to the cluster. The coordinator is the oldest member in the cluster.
If the coordinator leaves the cluster, the second oldest member gets elected as the coordinator.

Olric has a component called **rebalancer** which is responsible for keeping underlying data structures consistent:

* Works on every node,
* When a node joins or leaves, the cluster coordinator pushes the new partition table. Then, the **rebalancer** runs immediately and moves the partitions and backups to their new hosts,
* Merges fragmented partitions.

Partitions have a concept called **owners list**. When a node joins or leaves the cluster, a new primary owner may be assigned by the 
coordinator. At any time, a partition may have one or more partition owners. If a partition has two or more owners, this is called **fragmented partition**. 
The last added owner is called **primary owner**. Write operation is only done by the primary owner. The previous owners are only used for read and delete.

When you read a key, the primary owner tries to find the key on itself, first. Then, queries the previous owners and backups, respectively.
The delete operation works the same way.

The data(distributed map objects) in the fragmented partition is moved slowly to the primary owner by the **rebalancer**. Until the move is done,
the data remains available on the previous owners. The DMap methods use this list to query data on the cluster.

*Please note that, 'multiple partition owners' is an undesirable situation and the **rebalancer** component is designed to fix that in a short time.*

### Consistency and Replication Model

**Olric is an AP product** in the context of [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem), which employs the combination of primary-copy 
and [optimistic replication](https://en.wikipedia.org/wiki/Optimistic_replication) techniques. With optimistic replication, when the partition owner 
receives a write or delete operation for a key, applies it locally, and propagates it to backup owners.

This technique enables Olric clusters to offer high throughput. However, due to temporary situations in the system, such as network
failure, backup owners can miss some updates and diverge from the primary owner. If a partition owner crashes while there is an
inconsistency between itself and the backups, strong consistency of the data can be lost.

Two types of backup replication are available: **sync** and **async**. Both types are still implementations of the optimistic replication
model.

* **sync**: Blocks until write/delete operation is applied by backup owners.
* **async**: Just fire & forget.

#### Last-write-wins conflict resolution

Every time a piece of data is written to Olric, a timestamp is attached by the client. Then, when Olric has to deal with conflict data in the case 
of network partitioning, it simply chooses the data with the most recent timestamp. This called LWW conflict resolution policy.

#### PACELC Theorem

From Wikipedia:

> In theoretical computer science, the [PACELC theorem](https://en.wikipedia.org/wiki/PACELC_theorem) is an extension to the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem). It states that in case of network partitioning (P) in a 
> distributed computer system, one has to choose between availability (A) and consistency (C) (as per the CAP theorem), but else (E), even when the system is 
> running normally in the absence of partitions, one has to choose between latency (L) and consistency (C).

In the context of PACELC theorem, Olric is a **PA/EC** product. It means that Olric is considered to be **consistent** data store if the network is stable. 
Because the key space is divided between partitions and every partition is controlled by its primary owner. All operations on DMaps are redirected to the 
partition owner. 

In the case of network partitioning, Olric chooses **availability** over consistency. So that you can still access some parts of the cluster when the network is unreliable, 
but the cluster may return inconsistent results.  

Olric implements read-repair and quorum based voting system to deal with inconsistencies in the DMaps. 

Readings on PACELC theorem:
* [Please stop calling databases CP or AP](https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html)
* [Problems with CAP, and Yahoo’s little known NoSQL system](https://dbmsmusings.blogspot.com/2010/04/problems-with-cap-and-yahoos-little.html)
* [A Critique of the CAP Theorem](https://arxiv.org/abs/1509.05393)
* [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html)

#### Read-Repair on DMaps

Read repair is a feature that allows for inconsistent data to be fixed at query time. Olric tracks every write operation with a timestamp value and assumes 
that the latest write operation is the valid one. When you want to access a key/value pair, the partition owner retrieves all available copies for that pair
and compares the timestamp values. The latest one is the winner. If there is some outdated version of the requested pair, the primary owner propagates the latest
version of the pair. 

Read-repair is disabled by default for the sake of performance. If you have a use case that requires a more strict consistency control than a distributed caching 
scenario, you can enable read-repair via configuration. 

#### Quorum-based replica control

Olric implements Read/Write quorum to keep the data in a consistent state. When you start a write operation on the cluster and write quorum (W) is 2, 
the partition owner tries to write the given key/value pair on its own data storage and on the replica nodes. If the number of successful write operations 
is below W, the primary owner returns `ErrWriteQuorum`. The read flow is the same: if you have R=2 and the owner only access one of the replicas, 
it returns `ErrReadQuorum`.

#### Simple Split-Brain Protection

Olric implements a technique called *majority quorum* to manage split-brain conditions. If a network partitioning occurs and some of the members
lost the connection to rest of the cluster, they immediately stops functioning and return an error to incoming requests. This behaviour is controlled by
`MemberCountQuorum` parameter. It's default `1`. 

When the network healed, the stopped nodes joins again the cluster and fragmented partitions is merged by their primary owners in accordance with 
*LWW policy*. Olric also implements an *ownership report* mechanism to fix inconsistencies in partition distribution after a partitioning event. 

### Eviction
Olric supports different policies to evict keys from distributed maps. 

#### Expire with TTL
Olric implements TTL eviction policy. It shares the same algorithm with [Redis](https://redis.io/commands/expire#appendix-redis-expires):

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

#### Expire with MaxIdleDuration

Maximum time for each entry to stay idle in the DMap. It limits the lifetime of the entries relative to the time of the last read 
or write access performed on them. The entries whose idle period exceeds this limit are expired and evicted automatically. 
An entry is idle if no Get, Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration feature varies by 
preferred deployment method. 

#### Expire with LRU

Olric implements LRU eviction method on DMaps. Approximated LRU algorithm is borrowed from Redis. The Redis authors proposes the following algorithm:

> It is important to understand that the eviction process works like this:
> 
> * A client runs a new command, resulting in more data added.
> * Redis checks the memory usage, and if it is greater than the maxmemory limit , it evicts keys according to the policy.
> * A new command is executed, and so forth.
>
> So we continuously cross the boundaries of the memory limit, by going over it, and then by evicting keys to return back under the limits.
>
> If a command results in a lot of memory being used (like a big set intersection stored into a new key) for some time the memory 
> limit can be surpassed by a noticeable amount. 
>
> **Approximated LRU algorithm**
>
> Redis LRU algorithm is not an exact implementation. This means that Redis is not able to pick the best candidate for eviction, 
> that is, the access that was accessed the most in the past. Instead it will try to run an approximation of the LRU algorithm, 
> by sampling a small number of keys, and evicting the one that is the best (with the oldest access time) among the sampled keys.

Olric tracks access time for every DMap instance. Then it picks and sorts some configurable amount of keys to select keys for eviction.
Every node runs this algorithm independently. The access log is moved along with the partition when a network partition is occured.

#### Configuration of eviction mechanisms


### Lock Implementation

The DMap implementation is already thread-safe to meet your thread safety requirements. When you want to have more control on the
concurrency, you can use **LockWithTimeout** and **Lock** methods. Olric borrows the locking algorithm from Redis. Redis authors propose
the following algorithm:

> The command <SET resource-name anystring NX EX max-lock-time> is a simple way to implement a locking system with Redis.
>
> A client can acquire the lock if the above command returns OK (or retry after some time if the command returns Nil), and remove the lock just using DEL.
>
> The lock will be auto-released after the expire time is reached.
>
> It is possible to make this system more robust modifying the unlock schema as follows:
>
> Instead of setting a fixed string, set a non-guessable large random string, called token.
> Instead of releasing the lock with DEL, send a script that only removes the key if the value matches.
> This avoids that a client will try to release the lock after the expire time deleting the key created by another client that acquired the lock later.

Equivalent of`SETNX` command in Olric is `PutIf(key, value, IfNotFound)`. Lock and LockWithTimeout commands are properly implements
the algorithm which is proposed above. 

You should know that this implementation is subject to the clustering algorithm. Olric is an AP product. So there is no guarantee about reliability. 

**I recommend the lock implementation to be used for efficiency purposes in general, instead of correctness.**

### Storage Engine

Olric implements an append-only log file, indexed with a builtin map. It creates new tables and evacuates existing data to the new ones if it needs to shrink or expand. 

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
	"github.com/buraksezer/olric/config"
)

type customType struct {
	Field1 string
	Field2 uint64
}

func main() {
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// Put 10 items into the DMap object.
	dm, err := db.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}
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
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	err = db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}

```

## Contributions

Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

## License

The Apache License, Version 2.0 - see LICENSE for more details.

## About the name

The inner voice of Turgut Özben who is the main character of [Oğuz Atay's masterpiece -The Disconnected-](https://www.bariscayli.com/single-post/2016/12/20/Tutunamayanlar---The-Disconnected).
