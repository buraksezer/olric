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

// Pretty printer for Olric stats

package main

import (
	"flag"
	"fmt"
	"github.com/buraksezer/olric/cli"
	"io/ioutil"
	"log"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-stats/query"
	"github.com/sean-/seed"
)

const defaultAddr string = "127.0.0.1:3320"

var usage = `Pretty printer for Olric stats

Usage: 
  olric-stats [flags] ...

Flags:
  -h -help                      
      Shows this screen.
  -v -version                   
      Shows version information.
  -a -addr
      Server URI. Default: %s.
  -r -runtime
      Runtime stats, including runtime.MemStats.
  -b -backup
      Query backup partitions.
  -p -partID
      Partition ID to query.
  -d -dump
      Dump stats data in JSON format.
  -t -timeout
      Specifies a time limit for requests and dial made by Olric client.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues`

var (
	showHelp    bool
	showVersion bool
	backup      bool
	dump        bool
	runstats    bool
	partID      int
	addr        string
	timeout     string
)

func main() {
	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	cli.BoolVar(f, &showHelp, "help", "h", false)
	cli.BoolVar(f, &showVersion, "version", "v", false)

	cli.StringVar(f, &timeout, "timeout", "t", "10s")

	cli.IntVar(f, &partID, "partID", "p", -1)

	cli.StringVar(f, &addr, "addr", "a", defaultAddr)

	cli.BoolVar(f, &backup, "backup", "b", false)
	cli.BoolVar(f, &runstats, "runtime", "r", false)
	cli.BoolVar(f, &dump, "dump", "d", false)

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		logger.Fatalf("Failed to parse flags: %v\n", err)
	}

	if showVersion {
		fmt.Printf("olric-load %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		logger.Printf(usage, defaultAddr, runtime.Version())
		return
	}

	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()

	q, err := query.New(addr, timeout, logger)
	if err != nil {
		logger.Fatalf("Failed to run olric-stats: %v", err)
	}

	if dump {
		err = q.Dump()
		if err != nil {
			logger.Fatalf("Failed to run olric-stats: %v", err)
		}
		return
	}

	if runstats {
		err = q.PrintRuntimeStats()
	} else if partID == -1 {
		err = q.PrintRawStats(backup)
	} else {
		err = q.PrintPartitionStats(uint64(partID), backup)
	}

	if err != nil {
		logger.Fatalf("Failed to run olric-stats: %v", err)
	}
}
