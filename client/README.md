# Golang Client

This package provides a Golang client for Olric Binary Protocol. The client is straightforward to use 
and it's identical with the client in embedded member mode.

This implementation also supports connection pooling by default. 

## Table of Contents

* [Setup](#setup)
* [Commands](#commands)
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
* [Sample Code](#sample-code)

## Setup

The client package has a simple `Config` structure.

```go
import "github.com/buraksezer/olric/client"
...
...
var clientConfig = &client.Config{
    Addrs:       []string{"localhost:3320"},
    Serializer:  serializer.NewMsgpackSerializer(),
    DialTimeout: 10 * time.Second,
    KeepAlive:   10 * time.Second,
    MaxConn:     100,
}
```

Now, we can create a new `Client` instance:

```go
c, err := client.New(clientConfig)
``` 

A client (it's `c` in our sample) instance should be created one time in your program's life time. See [Sample Code](#sample-code) section 
to see it in action.

## Commands

Before starting, you need to create a new DMap instance: 

```go
dm := c.NewDMap("mydmap")
```

The methods under DMap struct is thread-safe. If you have an alive client instance, creating a DMap instance is not 
a costly operation. However, you still should be created only one DMap instance, if it's possible. There is no need to close 
DMap instances after use. 

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

* **olric.IfNotFound**: Only set the key if it does not already exist. It returns `olric.ErrFound` if the key already exist.

* **olric.IfFound**: Only set the key if it already exist.It returns `olric.ErrKeyNotFound` if the key does not exist.

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

The key has to be `string`. Value type is arbitrary. It is safe to modify the contents of the arguments after PutEx 
returns but not before.


Flag argument currently has two different options:

* **olric.IfNotFound**: Only set the key if it does not already exist. It returns `olric.ErrFound` if the key already exist.

* **olric.IfFound**: Only set the key if it already exist.It returns `olric.ErrKeyNotFound` if the key does not exist.

Sample use:

```go
err := dm.PutIfEx("my-key", "my-value", time.Second, IfNotFound)
```

### Get

Get gets the value for the given key. It returns `olric.ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

```go
value, err := dm.Get("my-key")
```

It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.

### Expire

Expire updates the expiry for the given key. It returns `olric.ErrKeyNotFound` if the DB does not contains the key. It's thread-safe.

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

It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.


```go
ctx, err := dm.Lock("lock.foo", time.Second)
```

It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline. You should keep `LockContext` (as ctx) 
value to call **Unlock** method to release the lock.

**You should know that the locks are approximate, and only to be used for non-critical purposes.**

### Unlock

Unlock releases an acquired lock for the given key. It returns `olric.ErrNoSuchLock` if there is no lock for the given key.

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
data, err := c.Stats()
```

See `olric/stats/stats.go` for detailed info about the metrics.

### Ping 

Ping sends a dummy protocol messsage to the given host. This is useful to measure RTT between hosts. It also can be used as aliveness check.

```go
err := c.Ping()
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

## Serialization

All data in an Olric cluster has to be serialized into byte form before transferring or storing. You can freely choice 
the serialization format for your data. **Msgpack**, **Gob** and **JSON** formats are supported by default. Any serialization 
format can be adapted by implementing the following interface:

```go
// Serializer interface responsible for encoding/decoding values to transmit over network between Olric nodes.
type Serializer interface {
	// Marshal encodes v and returns a byte slice and possible error.
	Marshal(v interface{}) ([]byte, error)

	// Unmarshal decodes the encoded data and stores the result in the value pointed to by v.
	Unmarshal(data []byte, v interface{}) error
}
```

The default serializer is Gob serializer. The Msgpack serializer is the fastest one. While
creating a new client configuration:

```go
...
Serializer: serializer.NewMsgpackSerializer()
...
```

Available ones:
* serializer.NewMsgpackSerializer()
* serializer.NewJSONSerializer()
* serializer.NewGobSerializer()
 
## Sample Code

Here is a simple example for quick experimenting. Please note that you need to run an **olricd** instance at 
TCP port 3320 to run this sample. 

```go
package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/serializer"
)

func main() {
	cc := &client.Config{
		Addrs:      []string{"127.0.0.1:3320"},
		MaxConn:    10,
		Serializer: serializer.NewMsgpackSerializer(),
	}

	// Create a new client instance
	c, err := client.New(cc)
	if err != nil {
		log.Fatalf("Olric client returned error: %s", err)
	}
	defer c.Close()

	// Create a DMap instance
	dm := c.NewDMap("my-dmap")
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i * 2)
		if err = dm.Put(key, value); err != nil {
			log.Fatalf("put failed for %s: %s", key, err)
		}
		fmt.Printf("[PUT] Key: %s, Value: %s\n", key, value)
	}

	fmt.Printf("\n")

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		value, err := dm.Get(key)
		if err != nil {
			log.Fatalf("get failed for %s: %s", err)
		}
		fmt.Printf("[GET] Key: %s, Value: %s\n", key, value)
	}
}
```