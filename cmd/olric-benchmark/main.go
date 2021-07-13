// Copyright 2018-2021 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Dummy load generator for Olric

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-benchmark/benchmark"
	"github.com/sean-/seed"
)

const (
	defaultConnections int    = 50
	defaultRequests    int    = 100000
	defaultSerializer  string = "gob"
	defaultAddress     string = "127.0.0.1:3320"
)

func usage() {
	var msg = `Usage: olric-benchmark [options] ...

Benchmark tool for Olric.

Options:
  -h, --help        Print this message and exit.
  -v, --version     Print the version number and exit.
  -a  --address     Network address of the server in <host:port> format.
                    Default: 127.0.0.1:3320
  -t  --timeout     Set time limit for requests and dial made by the client.
                    Default: 10ms
  -s  --serializer  Serialization format. Built-in: gob, json, msgpack.
                    Default: gob
  -T  --test        Name of the test to run.
                    Available test: put, get, delete, incr, decr.
  -r  --requests    Total number of requests.
                    Default: 100000
  -c  --connections Number of parallel connections.
                    Default: 50
  -k  --keep-going  Continue as much as possible after an error.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues
`
	_, err := fmt.Fprintf(os.Stdout, msg, runtime.Version())
	if err != nil {
		panic(err)
	}
}

func init() {
	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()
}

var version, help bool

func main() {
	args := &benchmark.Args{}

	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	f.BoolVar(&help, "h", false, "")
	f.BoolVar(&help, "help", false, "")

	f.BoolVar(&version, "v", false, "")
	f.BoolVar(&version, "version", false, "")

	f.StringVar(&args.Timeout, "t", "10s", "")
	f.StringVar(&args.Timeout, "timeout", "10s", "")

	f.StringVar(&args.Address, "a", defaultAddress, "")
	f.StringVar(&args.Address, "address", defaultAddress, "")

	f.IntVar(&args.Requests, "r", defaultRequests, "")
	f.IntVar(&args.Requests, "requests", defaultRequests, "")

	f.StringVar(&args.Serializer, "s", defaultSerializer, "")
	f.StringVar(&args.Serializer, "serializer", defaultSerializer, "")

	f.StringVar(&args.Test, "T", "", "")
	f.StringVar(&args.Test, "test", "", "")

	f.IntVar(&args.Connections, "c", defaultConnections, "")
	f.IntVar(&args.Connections, "connections", defaultConnections, "")

	f.BoolVar(&args.KeepGoing, "k", false, "")
	f.BoolVar(&args.KeepGoing, "keep-going", false, "")

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		logger.Fatalf("Failed to parse flags: %v\n", err)
	}

	if version {
		fmt.Printf("olric-benchmark %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if help {
		usage()
		return
	}

	l, err := benchmark.New(args, logger)
	if err != nil {
		logger.Fatalf("olric-benchmark: %v\n", err)
	}

	err = l.Run(args.Test)
	if err != nil {
		logger.Fatalf("olric-benchmark: %v\n", err)
	}
}
