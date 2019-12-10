// Copyright 2019 Burak Sezer
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
	"github.com/buraksezer/olric/cmd/olric-load/loader"
	"github.com/sean-/seed"
)

const (
	defaultNumClients int    = 50
	defaultKeyCount   int    = 100
	defaultSerializer string = "gob"
	defaultAddrs      string = "127.0.0.1:3320"
)

var usage = `Dummy load generator for Olric

Usage: 
  olric-load [flags] ...

Flags:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.
  
  -s -serializer
      Specifies serialization format. Available formats: gob, json, msgpack. Default: %s.

  -a -addrs
      Comma separated server URIs. Default: %s.

  -c -command
      Command to run. Available commands: put, get, delete, incr, decr.
   
  -k -key-count
      Key count to load into the server.

  -n -num-clients
      Number of clients in parallel.

  -t -timeout
      Specifies a time limit for requests and dial made by Olric client.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues`

var (
	showHelp    bool
	showVersion bool
	addrs       string
	keyCount    int
	numClients  int
	timeout     string
	command     string
	serializer  string
)

func init() {
	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()
}

func main() {
	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.BoolVar(&showHelp, "h", false, "")
	f.BoolVar(&showHelp, "help", false, "")

	f.BoolVar(&showVersion, "v", false, "")
	f.BoolVar(&showVersion, "version", false, "")

	f.StringVar(&timeout, "t", "10s", "")
	f.StringVar(&timeout, "timeout", "10s", "")

	f.StringVar(&addrs, "a", defaultAddrs, "")
	f.StringVar(&addrs, "addrs", defaultAddrs, "")

	f.IntVar(&keyCount, "k", defaultKeyCount, "")
	f.IntVar(&keyCount, "key-count", defaultKeyCount, "")

	f.StringVar(&serializer, "s", defaultSerializer, "")
	f.StringVar(&serializer, "serializer", defaultSerializer, "")

	f.StringVar(&command, "c", "", "")
	f.StringVar(&command, "command", "", "")

	f.IntVar(&numClients, "n", defaultNumClients, "")
	f.IntVar(&numClients, "num-clients", defaultNumClients, "")

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		logger.Fatalf("Failed to parse flags: %v\n", err)
	}

	if showVersion {
		fmt.Printf("olric-load %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		logger.Printf(usage, defaultSerializer, defaultAddrs, runtime.Version())
		return
	}

	l, err := loader.New(addrs, timeout, serializer, numClients, keyCount, logger)
	if err != nil {
		logger.Fatalf("Failed to create a new loader instance: %v\n", err)
	}
	err = l.Run(command)
	if err != nil {
		logger.Fatalf("Failed to run olric-load: %v\n", err)
	}
}
