consistent
==========
[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/buraksezer/consistent) [![Build Status](https://travis-ci.org/buraksezer/consistent.svg?branch=master)](https://travis-ci.org/buraksezer/consistent) [![Coverage](http://gocover.io/_badge/github.com/buraksezer/consistent)](http://gocover.io/github.com/buraksezer/consistent) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/consistent)](https://goreportcard.com/report/github.com/buraksezer/consistent) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This library provides a consistent hashing function which simultaneously achieves both uniformity and consistency. 

For detailed information about the concept, you should take a look at the following resources:

* [Consistent Hashing with Bounded Loads on Google Research Blog](https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)
* [Improving load balancing with a new consistent-hashing algorithm on Vimeo Engineering Blog](https://medium.com/vimeo-engineering-blog/improving-load-balancing-with-a-new-consistent-hashing-algorithm-9f1bd75709ed)
* [Consistent Hashing with Bounded Loads paper on arXiv](https://arxiv.org/abs/1608.01350)

Table of Content
----------------

- [Overview](#overview)
- [Install](#install)
- [Configuration](#configuration)
- [Usage](#usage)
- [Benchmarks](#benchmarks)
- [Examples](#examples)

Overview
--------

In this package's context, the keys are distributed among partitions and partitions are distributed among members as well. 

When you create a new consistent instance or call `Add/Remove`:

* The member's name is hashed and inserted it into the hash ring,
* Average load is calculated by the algorithm defined in the paper,
* Partitions are distributed among members by hashing partition IDs and any of them don't exceed the average load.

Average load cannot be exceeded. So if all the members are loaded at the maximum while trying to add a new member, it panics.

When you want to locate a key by calling `LocateKey`:

* The key(byte slice) is hashed,
* The result of the hash is mod by the number of partitions,
* The result of this modulo - `MOD(hash result, partition count)` - is the partition in which the key will be located,
* Owner of the partition is already determined before calling `LocateKey`. So it returns the partition owner immediately.

No memory is allocated by `consistent` except hashing when you want to locate a key.

Note that the number of partitions cannot be changed after creation. 

Install
-------

With a correctly configured Go environment:

```
go get github.com/buraksezer/consistent
```

You will find some useful usage samples in [examples](https://github.com/buraksezer/consistent/tree/master/_examples) folder.

Configuration
-------------

```go
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Members are replicated on consistent hash ring. This number means that a member
	// how many times replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's 
        // blog post to learn about it.
	Load float64
}
```

Any hash algorithm can be used as hasher which implements Hasher interface. Please take a look at the *Sample* section for an example.

Usage
-----

`LocateKey` function finds a member in the cluster for your key:
```go
// With a properly configured and initialized consistent instance
key := []byte("my-key")
member := c.LocateKey(key)
```
It returns a thread-safe copy of the member you added before.

The second most probably used function is `GetClosestN`. 

```go
// With a properly configured and initialized consistent instance

key := []byte("my-key")
members, err := c.GetClosestN(key, 2)
```

This may be useful to find backup nodes to store your key.

Benchmarks
----------
On an early 2015 Macbook:

```
BenchmarkAddRemove-4     	  100000	     22006 ns/op
BenchmarkLocateKey-4     	 5000000	       252 ns/op
BenchmarkGetClosestN-4   	  500000	      2974 ns/op
```

Examples
--------

The most basic use of consistent package should be like this. For detailed list of functions, [visit godoc.org.](https://godoc.org/github.com/buraksezer/consistent)
More sample code can be found under [_examples](https://github.com/buraksezer/consistent/tree/master/_examples).

```go
package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// In your code, you probably have a custom data type 
// for your cluster members. Just add a String function to implement 
// consistent.Member interface.
type myMember string

func (m myMember) String() string {
	return string(m)
}

// consistent package doesn't provide a default hashing function. 
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func main() {
	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	node1 := myMember("node1.olricmq.com")
	c.Add(node1)

	node2 := myMember("node2.olricmq.com")
	c.Add(node2)

	key := []byte("my-key")
	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions is already distributed among members by Add function.
	owner := c.LocateKey(key)
	fmt.Println(owner.String())
	// Prints node2.olricmq.com
}
```

Another useful example is `_examples/relocation_percentage.go`. It creates a `consistent` object with 8 members and distributes partitions among them. Then adds 9th member, 
here is the result with a proper configuration and hash function:

```
bloom:consistent burak$ go run _examples/relocation_percentage.go
partID: 218 moved to node2.olricmq from node0.olricmq
partID: 173 moved to node9.olricmq from node3.olricmq
partID: 225 moved to node7.olricmq from node0.olricmq
partID:  85 moved to node9.olricmq from node7.olricmq
partID: 220 moved to node5.olricmq from node0.olricmq
partID:  33 moved to node9.olricmq from node5.olricmq
partID: 254 moved to node9.olricmq from node4.olricmq
partID:  71 moved to node9.olricmq from node3.olricmq
partID: 236 moved to node9.olricmq from node2.olricmq
partID: 118 moved to node9.olricmq from node3.olricmq
partID: 233 moved to node3.olricmq from node0.olricmq
partID:  50 moved to node9.olricmq from node4.olricmq
partID: 252 moved to node9.olricmq from node2.olricmq
partID: 121 moved to node9.olricmq from node2.olricmq
partID: 259 moved to node9.olricmq from node4.olricmq
partID:  92 moved to node9.olricmq from node7.olricmq
partID: 152 moved to node9.olricmq from node3.olricmq
partID: 105 moved to node9.olricmq from node2.olricmq

6% of the partitions are relocated
```

Moved partition count is highly depends on your configuration and quailty of hash function. You should modify the configuration to find an optimum set of configuration
for your system.

`_examples/load_distribution.go` is also useful to understand load distribution. It creates a `consistent` object with 8 members and locates 1M key. It also calculates average 
load which cannot be exceeded by any member. Here is the result:

```
Maximum key count for a member should be around this:  147602
member: node2.olricmq, key count: 100362
member: node5.olricmq, key count: 99448
member: node0.olricmq, key count: 147735
member: node3.olricmq, key count: 103455
member: node6.olricmq, key count: 147069
member: node1.olricmq, key count: 121566
member: node4.olricmq, key count: 147932
member: node7.olricmq, key count: 132433
```

Average load can be calculated by using the following formula:

```
load := (consistent.AverageLoad() * float64(keyCount)) / float64(config.PartitionCount)
```

Contributions
-------------
Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

License
-------
MIT License, - see LICENSE for more details.
