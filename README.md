# Olric [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Olric%3A+Distributed+and+in-memory+key%2Fvalue+database.+It+can+be+used+both+as+an+embedded+Go+library+and+as+a+language-independent+service.+&url=https://github.com/buraksezer/olric&hashtags=golang,distributed,database)

[![Go Reference](https://pkg.go.dev/badge/github.com/buraksezer/olric.svg)](https://pkg.go.dev/github.com/buraksezer/olric) [![Coverage Status](https://coveralls.io/repos/github/buraksezer/olric/badge.svg?branch=master)](https://coveralls.io/github/buraksezer/olric?branch=master) [![Build Status](https://travis-ci.org/buraksezer/olric.svg?branch=master)](https://travis-ci.org/buraksezer/olric) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/olric)](https://goreportcard.com/report/github.com/buraksezer/olric) [![Discord](https://img.shields.io/discord/721708998021087273.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://discord.gg/ahK7Vjr8We) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Distributed cache and in-memory key/value data store. It can be used both as an embedded Go library and as a language-independent service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across a cluster of computers. 

See [Docker](#docker) and [Sample Code](#sample-code) sections to get started! 

Join our [Discord server!](https://discord.gg/ahK7Vjr8We)

The current production version is [v0.3.12](https://github.com/buraksezer/olric/tree/v0.3.12)

## At a glance

* Designed to share some transient, approximate, fast-changing data between servers,
* Embeddable but can be used as a language-independent service with *olricd*,
* Supports different eviction algorithms,
* Fast binary protocol,
* Highly available and horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control (Read/Write quorums),
* Supports atomic operations,
* Supports [distributed queries](#query) on keys,
* Provides a plugin interface for service discovery daemons,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),
* Supports [distributed topic](#distributed-topic) data structure,

## Possible Use Cases

With this feature set, Olric is suitable to use as a distributed cache. But it also provides distributed topics, data replication, 
failure detection and simple anti-entropy services. So it can be used as an ordinary key/value data store to scale your cloud application.

## Table of Contents

* [Features](#features)
* [Planned Features](#planned-features)
* [Support](#support)
* [Installing](#installing)
  * [Docker](#docker)
  * [Kubernetes](#kubernetes)
  * [Working with Docker Compose](#working-with-docker-compose)
* [Operation Modes](#operation-modes)
  * [Embedded Member](#embedded-member)
  * [Client-Server](#client-server)
* [Tooling](#tooling)
  * [olricd](#olricd)
  * [olric-cli](#olric-cli)
  * [olric-stats](#olric-stats)
  * [olric-benchmark](#olric-benchmark)
* [Usage](#usage)
  * [Distributed Map](#distributed-map)
    * [Put](#put)
    * [PutIf](#putif)
    * [PutEx](#putex)
    * [PutIfEx](#putifex)
    * [Get](#get)
    * [GetEntry](#getentry)
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
  * [Distributed Topic](#distributed-topic)
    * [Publish](#publish)
    * [AddListener](#addlistener)
    * [RemoveListener](#removelistener)
    * [Destroy](#destroy)
* [Serialization](#serialization)
* [Golang Client](#golang-client)
* [Configuration](#configuration)
    * [Embedded Member Mode](#embedded-member-mode)
      * [Manage the configuration in YAML format](#manage-the-configuration-in-yaml-format)
    * [Client-Server Mode](#client-server-mode)
    * [Network Configuration](#network-configuration)
    * [Service discovery](#service-discovery)
    * [Timeouts](#timeouts)
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
* Different eviction policies: LRU, MaxIdleDuration and Time-To-Live (TTL),
* Highly available,
* Horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Distributes load fairly among cluster members with a [consistent hash function](https://github.com/buraksezer/consistent),
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control,
* Thread-safe by default,
* Supports [distributed queries](#query) on keys,
* Provides a plugin interface for service discovery daemons and cloud providers,
* Provides a command-line-interface to access the cluster directly from the terminal,
* Supports different serialization formats. Gob, JSON and MessagePack are supported out of the box,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),
* Supports [distributed topic](#distributed-topic) data structure,

See [Architecture](#architecture) section to see details.

## Planned Features

* Distributed queries over keys and values,
* Persistence with AOF (Append Only File),
* Anti-entropy system to repair inconsistencies in DMaps,
* Eviction listeners by using Publish/Subscribe,
* Memcached interface,
* Client implementations for different languages: Java, Python and JavaScript.

## Support

You feel free to ask any questions about Olric and possible integration problems.

* [Discord server](https://discord.gg/ahK7Vjr8We)
* [Mail group on Google Groups](https://groups.google.com/forum/#!forum/olric-user)
* [GitHub Discussions](https://github.com/buraksezer/olric/discussions)

You also feel free to open an issue on GitHub to report bugs and share feature requests.

## Installing

With a correctly configured Golang environment:

```
go get -u github.com/buraksezer/olric
```

Then, install olricd and its siblings:

```
go install -v ./cmd/*
```

Now you should access **olricd**, **olric-stats**, **olric-cli** and **olric-benchmark** on your path. You can just run olricd
to start experimenting: 

```
olricd -c cmd/olricd/olricd.yaml
```

See [Configuration](#configuration) section to create your cluster properly.

### Docker

You can launch olricd Docker container by running the following command. 

```bash
docker run -p 3320:3320 olricio/olricd:latest
``` 

This command will pull olricd Docker image and run a new Olric Instance. You should know that the container exposes 
`3320` and `3322` ports. 

Now, you can access the instance by using `olric-cli`. You can build `olric-cli` by using the following command:

```bash
go get -u github.com/buraksezer/olric/cmd/olric-cli
```

Now you are able to connect the olricd server:

```bash
olric-cli
[127.0.0.1:3320] »
```

Give `help` command to see available commands. Olric has a dedicated repository for Docker-related resources. Please take a look at
[buraksezer/olric-docker](https://github.com/buraksezer/olric-docker) for more information.

### Kubernetes

Olric is able to discover peers automatically on Kubernetes platform via [olric-cloud-plugin](https://github.com/buraksezer/olric-cloud-plugin). We have a very simple 
Kubernetes setup right now. In the near future, this will be a major development/improvement area for Olric. 

If you have a running Kubernetes cluster, you can use the following command to deploy a new Olric cluster with 3 nodes:

```bash
kubectl apply -f https://raw.githubusercontent.com/buraksezer/olric-kubernetes/master/olricd.yaml
``` 

If everything goes well, you should see something like that:

```bash
kubectl get pods
NAME                      READY   STATUS    RESTARTS   AGE
dnsutils                  1/1     Running   0          20d
olricd-6c7f54d445-ndm8v   1/1     Running   0          34s
olricd-6c7f54d445-s6g6r   1/1     Running   0          34s
olricd-6c7f54d445-vjkhf   1/1     Running   0          34s
```

Now we have an Olric cluster on Kubernetes with 3 nodes. One of them is the cluster coordinator and manages the routing table for rest of the cluster. 

Deploy `olric-debug` to reach the cluster:

```bash
kubectl apply -f https://raw.githubusercontent.com/buraksezer/olric-kubernetes/master/olric-debug.yaml
```

Verify whether `olric-debug` pod works or not:

```bash
kubectl get pods
NAME                      READY   STATUS    RESTARTS   AGE
...
olric-debug               1/1     Running   0          54s
...
```

Get a shell to the running container:

```bash
kubectl exec -it olric-debug -- /bin/sh
```

Now you have a running Alpine Linux setup on Kubernetes. It includes `olric-cli`, `olric-benchmark` and `olric-stats` commands. 

```bash
/go/src/github.com/buraksezer/olric # olric-cli -a olricd.default.svc.cluster.local:3320
[olricd.default.svc.cluster.local:3320] » use users
use users
[olricd.default.svc.cluster.local:3320] » put buraksezer {"_id": "06054057", "name": "Burak", "surname": "Sezer", "job": "Engineer"}
[olricd.default.svc.cluster.local:3320] » get buraksezer
{"_id": "06054057", "name": "Burak", "surname": "Sezer", "profession": "Engineer"}
[olricd.default.svc.cluster.local:3320] »
```

Congrats! 

Bringing Olric into Kubernetes will be a major development area in the next releases.

### Working with Docker Compose

We provide a multi-container environment to test, develop and deploy Olric clusters. [Here is the documentation.](docker/README.md)

## Operation Modes

Olric has two different operation modes. 

### Embedded Member

In Embedded Member Mode, members include both the application and Olric data and services. The advantage of the Embedded 
Member Mode is having a low-latency data access and locality.

### Client-Server

In the Client-Server deployment, Olric data and services are centralized in one or more servers, and they are 
accessed by the application through clients. You can have a cluster of servers that can be independently created 
and scaled. Your clients communicate with these members to reach to Olric data and services on them.

Client-Server deployment has advantages including more predictable and reliable performance, easier identification 
of problem causes and, most importantly, better scalability. When you need to scale in this deployment type, just add more 
Olric server members. You can address client and server scalability concerns separately. 

See [olricd](#olricd) section to get started.

Currently, we only have the official Golang client. A possible Python implementation is on the way. After stabilizing the
Olric Binary Protocol, the others may appear quickly.

## Tooling

Olric comes with some useful tools to interact with the cluster. 

### olricd

With olricd, you can create an Olric cluster with a few commands. This is how to install olricd:

```bash
go get -u github.com/buraksezer/olric/cmd/olricd
```

Let's create a cluster with the following:

```
olricd -c <YOUR_CONFIG_FILE_PATH>
```

olricd also supports `OLRICD_CONFIG` environment variable to set configuration. Just like that: 

```
OLRICD_CONFIG=<YOUR_CONFIG_FILE_PATH> olricd
```

Olric uses [hashicorp/memberlist](https://github.com/hashicorp/memberlist) for failure detection and cluster membership. 
Currently, there are different ways to discover peers in a cluster. You can use a static list of nodes in your `olricd.yaml` 
file. It's ideal for development and test environments. Olric also supports Consul, Kubernetes and well-known cloud providers
for service discovery. Please take a look at [Service Discovery](#service-discovery) section for further information.

You can find a sample configuration file under `cmd/olricd/olricd.yaml`. 

See [Client-Server](#client-server) section to get more information about this deployment scenario.

### olric-cli

olric-cli is the Olric command line interface, a simple program that allows to send commands to Olric, and read the replies 
sent by the server, directly from the terminal.

In order to install `olric-cli`:

```bash
go get -u github.com/buraksezer/olric/cmd/olric-cli
```

olric-cli has an interactive (REPL) mode just like `redis-cli`:

```
olric-cli
[127.0.0.1:3320] >> use mydmap
use mydmap
[127.0.0.1:3320] >> get mykey
myvalue
[127.0.0.1:3320] >>
```

The interactive mode also keeps command history.  It's possible to send protocol commands as command line arguments:

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

olric-stats calls `Stats` command on a given cluster member and prints the result. 
The results from the member also includes the Go runtime metrics and statistics from 
hosted primary and backup partitions. 

You should know that all the statistics are belonged to the current member. 

In order to install `olric-stats`:

```bash
go get -u github.com/buraksezer/olric/cmd/olric-stats
```

Statistics about a partition:

```
olric-stats --partitions --id 69
PartID: 69
  Owner: olric.node:3320
  Previous Owners: not found
  Backups: not found
  DMap count: 1
  DMaps:
    Name: olric-benchmark-test
    Length: 1374
    Allocated: 1048576
    Inuse: 47946
    Garbage: 0
```

In order to get detailed statistics about the Go runtime, you should call `olric-stats -a <ADDRESS> -r`. 
In order to get more details about the command, call `olric-stats -h`.

See [stats/stats.go](stats/stats.go) file to get detailed information about the statistics.

### olric-benchmark

olric-benchmark simulates running commands done by N clients at the same time sending M total queries. It measures response time.

In order to install `olric-benchmark`:

```bash
go get -u github.com/buraksezer/olric/cmd/olric-benchmark
```

The following command calls `Put` command for 1M keys on `127.0.0.1:3320` (it's default) and uses `msgpack` for serialization.

```
olric-benchmark -a 192.168.1.3:3320 -s msgpack -r 1000000 -T put
### STATS FOR COMMAND: PUT ###
Serializer is msgpack
1000000 requests completed in 6.943316278s
50 parallel clients

98.36% <= 0 milliseconds
99.50% <= 1 milliseconds
99.79% <= 2 milliseconds
99.91% <= 3 milliseconds
99.95% <= 4 milliseconds
99.96% <= 5 milliseconds
99.96% <= 6 milliseconds
99.97% <= 7 milliseconds
99.98% <= 10 milliseconds
99.99% <= 15 milliseconds
100.00% <= 96 milliseconds

144023.397460 requests per second
```

In order to get more details about the command, call `olric-benchmark -h`.

## Usage

Olric is designed to work efficiently with the minimum amount of configuration. So the default configuration should be enough for experimenting:

```go
db, err := olric.New(config.New("local"))
```

This creates an Olric object without running any server at background. In order to run Olric, you need to call **Start** method.

```go
err := db.Start()
```

When you call **Start** method, your process joins the cluster and will be responsible for some parts of the data. This call blocks
indefinitely. So you may need to run it in a goroutine. Of course, this is just a single-node instance, because you didn't give any
configuration.

When you want to leave the cluster, just need to call **Shutdown** method:

```go
err := db.Shutdown(context.Background())
```

This will stop background tasks and servers. Finally purges in-memory data and quits.

***Please note that this section aims to document DMap API in embedded member mode.*** If you prefer to use Olric in 
Client-Server mode, please jump to [Golang Client](#golang-client) section. 

### Distributed Map

Create a **DMap** instance:

```go
dm, err := db.NewDMap("my-dmap")
```
 
### Put

Put sets the value for the given key. It overwrites any previous value for that key, and it's thread-safe.

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
err := dm.PutIf("my-key", "my-value", IfNotFound)
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

It is safe to modify the contents of the returned value.

### GetEntry

Get gets the value for the given key with its metadata. It returns `ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

```go
entry, err := dm.GetEntry("my-key")
```

Definition of `Entry`:

```go
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}
```

It is safe to modify the contents of the returned value. 

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

It returns immediately, if it acquires the lock for the given key. Otherwise, it waits until deadline. You should keep `LockContext` (as ctx) 
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

Ping sends a dummy protocol message to the given host. This is useful to measure RTT between hosts. It also can be used as aliveness check.

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

Operations on key/value pairs are performed by the partition owner. In addition, atomic operations are guarded by a lock implementation which can be found under `internal/locker`. It means that 
Olric guaranties consistency of atomic operations, if there is no network partition. Basic flow for `Incr`:

* Acquire the lock for the given key,
* Call `Get` to retrieve the current value,
* Calculate the new value,
* Call `Put` to set the new value,
* Release the lock.

It's important to know that if you call `Put` and `GetPut` concurrently on the same key, this will break the atomicity.

`internal/locker` package is provided by [Docker](https://github.com/moby/moby).

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model)) product. So if your network is stable, all the operations on key/value 
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is stable. It's important to know that computer networks fail 
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you cannot tolerate losing strong consistency under network partitioning, 
you need to use a different tool for atomic operations.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html) and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight on this topic.

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

## Pipelining

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

There is no hard-limit on message count in a pipeline. You should set a convenient `KeepAlive` for large pipelines. Otherwise, you can get a timeout error.

The `Flush` method returns errors along with success messages. Furthermore, you need to know the command order for matching responses with requests.

### Distributed Topic

Distributed topic is an asynchronous messaging service that decouples services that produce events from services that process events. It has two delivery modes:

* **olric.UnorderedDelivery**: Messages are delivered in random order. It's good to distribute independent events in a distributed system.
* **olric.OrderedDelivery**: Messages are delivered in some order. Not implemented yet. 

You should know that:

* Communication between parties is one-to-many (fan-out). 
* All data is in-memory, and the published messages are not stored in the cluster.
* Fire&Forget: message delivery is not guaranteed.

Create a **DTopic** instance:

```go
dt, err := db.NewDTopic("my-topic", 0, olric.UnorderedDelivery)
```

### Publish

Publish sends a message to the given topic. It accepts any serializable type as message. 

```go
err := dt.Publish("my-message")
```

### AddListener

AddListener adds a new listener for the topic. Returns a listener ID or a non-nil error. The callback functions for this DTopic are run by parallel.

```go
listenerID, err := dt.AddListener(func(msg DTopicMessage) {
    fmt.Println("Message:", msg)
})
```

You have to store `listenerID` to remove the listener.

### RemoveListener

RemoveListener removes a listener with the given listenerID.

```go
err := dt.RemoveListener(listenerID)
```

### Destroy

Destroy a DTopic from the cluster. It stops background goroutines and releases underlying data structures.

```go
err := dt.Destroy()
```

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
The requests are distributed among available TCP connections using an algorithm called `round-robin`. In order to see detailed list of
configuration parameters, see [Olric documentation on GoDoc.org](https://godoc.org/github.com/buraksezer/olric).

The official Golang client has its dedicated documentation. Please take a look at [this](https://github.com/buraksezer/olric/tree/master/client#golang-client).

## Configuration

You should feel free to ask any questions about configuration and integration. Please see [Support](#support) section.

### Embedded-Member Mode

Olric provides a function to generate default configuration to use in embedded-member mode:

```go
import "github.com/buraksezer/olric/config"
...
c := config.New("local")
```

The `New` function takes a parameter called `env`. It denotes the network environment and consumed by [hashicorp/memberlist](https://github.com/hashicorp/memberlist). 
Default configuration is good enough for distributed caching scenario. In order to see all configuration parameters, please take a look at [this](https://godoc.org/github.com/buraksezer/olric/config).

See [Sample Code](#sample-code) section for an introduction.

#### Manage the configuration in YAML format

You can also import configuration from a YAML file by using the `Load` function:

```go
c, err := config.Load(path/to/olric.yaml)
```

A sample configuration file in YAML format can be found [here](https://github.com/buraksezer/olric/blob/master/cmd/olricd/olricd.yaml). This may be the most appropriate way to manage the Olric configuration.


### Client-Server Mode

Olric provides **olricd** to implement client-server mode. olricd gets a YAML file for the configuration. The most basic  functionality of olricd is that 
translating YAML configuration into Olric's configuration struct. A sample `olricd.yaml` file  is being provided [here](https://github.com/buraksezer/olric/blob/master/cmd/olricd/olricd.yaml).

### Network Configuration

In an Olric instance, there are two different TCP servers. One for Olric, and the other one is for memberlist. `BindAddr` is very
critical to deploy a healthy Olric node. There are different scenarios:

* You can freely set a domain name or IP address as `BindAddr` for both Olric and memberlist. Olric will resolve and use it to bind.
* You can freely set `localhost`, `127.0.0.1` or `::1` as `BindAddr` in development environment for both Olric and memberlist.
* You can freely set `0.0.0.0` as `BindAddr` for both Olric and memberlist. Olric will pick an IP address, if there is any.
* If you don't set `BindAddr`, hostname will be used, and it will be resolved to get a valid IP address.
* You can set a network interface by using `Config.Interface` and `Config.MemberlistInterface` fields. Olric will find an appropriate IP address for the given interfaces, if there is any.
* You can set both `BindAddr` and interface parameters. In this case Olric will ensure that `BindAddr` is available on the given interface.

You should know that Olric needs a single and stable IP address to function properly. If you don't know the IP address of the host at the deployment time, 
you can set `BindAddr` as `0.0.0.0`. Olric will very likely to find an IP address for you.

### Service Discovery

Olric provides a service discovery interface which can be used to implement plugins. 

We currently have a bunch of service discovery plugins for automatic peer discovery on cloud environments:

* [buraksezer/olric-consul-plugin](https://github.com/buraksezer/olric-consul-plugin) provides a plugin using Consul.
* [buraksezer/olric-cloud-plugin](https://github.com/buraksezer/olric-cloud-plugin) provides a plugin for well-known cloud providers. Including Kubernetes.
* [justinfx/olric-nats-plugin](https://github.com/justinfx/olric-nats-plugin) provides a plugin using nats.io

In order to get more info about installation and configuration of the plugins, see their GitHub page. 

### Timeouts

Olric nodes supports setting `KeepAlivePeriod` on TCP sockets. 

**Server-side:**

##### config.KeepAlivePeriod 

KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.

**Client-side:**
 
##### config.DialTimeout

Timeout for TCP dial. The timeout includes name resolution, if required. When using TCP, and the host in the address 
parameter resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
given an appropriate fraction of the time to connect.

##### config.ReadTimeout

Timeout for socket reads. If reached, commands will fail with a timeout instead of blocking. Use value -1 for no 
timeout and 0 for default. The default is config.DefaultReadTimeout

##### config.WriteTimeout

Timeout for socket writes. If reached, commands will fail with a timeout instead of blocking. The default is config.DefaultWriteTimeout

##### config.KeepAlive

KeepAlive specifies the interval between keep-alive probes for an active network connection. If zero, keep-alive probes 
are sent with a default value (currently 15 seconds), if supported by the protocol and operating system. Network protocols 
or operating systems that do not support keep-alives ignore this field. If negative, keep-alive probes are disabled.

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

Olric distributes data among partitions. Every partition is being owned by a cluster member and may have one or more backups for redundancy. 
When you read or write a DMap entry, you transparently talk to the partition owner. Each request hits the most up-to-date version of a
particular data entry in a stable cluster.

In order to find the partition which the key belongs to, Olric hashes the key and mod it with the number of partitions:

```
partID = MOD(hash result, partition count)
```

The partitions are being distributed among cluster members by using a consistent hashing algorithm. In order to get details, please see [buraksezer/consistent](https://github.com/buraksezer/consistent). 

When a new cluster is created, one of the instances is elected as the **cluster coordinator**. It manages the partition table: 

* When a node joins or leaves, it distributes the partitions and their backups among the members again,
* Removes empty previous owners from the partition owners list,
* Pushes the new partition table to all the members,
* Pushes the partition table to the cluster periodically.

Members propagate their birthdate(POSIX time in nanoseconds) to the cluster. The coordinator is the oldest member in the cluster.
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
receives a write or delete operation for a key, applies it locally, and propagates it to the backup owners.

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
scenario, you can enable read-repair via the configuration. 

#### Quorum-based replica control

Olric implements Read/Write quorum to keep the data in a consistent state. When you start a write operation on the cluster and write quorum (W) is 2, 
the partition owner tries to write the given key/value pair on its own data storage and on the replica nodes. If the number of successful write operations 
is below W, the primary owner returns `ErrWriteQuorum`. The read flow is the same: if you have R=2 and the owner only access one of the replicas, 
it returns `ErrReadQuorum`.

#### Simple Split-Brain Protection

Olric implements a technique called *majority quorum* to manage split-brain conditions. If a network partitioning occurs, and some of the members
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

Here is a simple configuration block for `olricd.yaml`: 

```
cache:
  numEvictionWorkers: 1
  maxIdleDuration: ""
  ttlDuration: "100s"
  maxKeys: 100000
  maxInuse: 1000000 # in bytes
  lRUSamples: 10
  evictionPolicy: "LRU" # NONE/LRU
```

You can also set cache configuration per DMap. Here is a simple configuration for a DMap named `foobar`:

```
dmaps:
  foobar:
    maxIdleDuration: "60s"
    ttlDuration: "300s"
    maxKeys: 500000 # in-bytes
    lRUSamples: 20
    evictionPolicy: "NONE" # NONE/LRU
```

If you prefer embedded-member deployment scenario, please take a look at [config#CacheConfig](https://godoc.org/github.com/buraksezer/olric/config#CacheConfig) and [config#DMapCacheConfig](https://godoc.org/github.com/buraksezer/olric/config#DMapCacheConfig) for the configuration.


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

You should know that this implementation is subject to the clustering algorithm. So there is no guarantee about reliability in the case of network partitioning. I recommend the lock implementation to be used for 
efficiency purposes in general, instead of correctness.

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model)) product. So if your network is stable, all the operations on key/value 
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is stable. It's important to know that computer networks fail 
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you cannot tolerate losing strong consistency under network partitioning, 
you need to use a different tool for locking.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html) and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight on this topic.
             
### Storage Engine

Olric implements an append-only log file, indexed with a builtin map (uint64 => uint64). It creates new tables and evacuates existing data to the new ones if it needs to shrink or expand. 

## Sample Code

The following snipped can be run on your computer directly. It's a single-node setup, of course:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

func main() {
	// Deployment scenario: embedded-member
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

	dm, err := db.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	// Magic starts here!
	fmt.Println("##")
	fmt.Println("Operations on a DMap instance:")
	err = dm.Put("string-key", "buraksezer")
	if err != nil {
		log.Fatalf("Failed to call Put: %v", err)
	}
	stringValue, err := dm.Get("string-key")
	if err != nil {
		log.Fatalf("Failed to call Get: %v", err)
	}
	fmt.Printf("Value for string-key: %v, reflect.TypeOf: %s\n", stringValue, reflect.TypeOf(stringValue))

	err = dm.Put("uint64-key", uint64(1988))
	if err != nil {
		log.Fatalf("Failed to call Put: %v", err)
	}
	uint64Value, err := dm.Get("uint64-key")
	if err != nil {
		log.Fatalf("Failed to call Get: %v", err)
	}
	fmt.Printf("Value for uint64-key: %v, reflect.TypeOf: %s\n", uint64Value, reflect.TypeOf(uint64Value))
	fmt.Println("##")

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

The inner voice of Turgut Özben who is the main character of [Oğuz Atay's masterpiece -The Disconnected-](https://www.themodernnovel.org/asia/other-asia/turkey/oguz-atay/the-disconnected/).
