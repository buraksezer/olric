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

// Pretty printer for Olric stats

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-stats/query"
	"github.com/sean-/seed"
)

const defaultAddress = "127.0.0.1:3320"

type arguments struct {
	help       bool
	version    bool
	runtime    bool
	partitions bool
	backup     bool
	dump       bool
	dmap       bool
	dtopic     bool
	network    bool
	members    bool
	address    string
	timeout    string
	id         int
}

func usage() {
	var msg = `Usage: olric-stats [options] ...

Inspect cluster state and per-node statistics.

Options:
  -h, --help	   Print this message and exit.
  -v, --version	   Print the version number and exit.
  -a  --address	   Network address of the server in <host:port> format.
                   Default: 127.0.0.1:3320
  -t  --timeout    Set time limit for requests and dial made by the client.
                   Default: 10ms
  -r  --runtime	   Print Go runtime stats. It calls runtime.ReadMemStats
                   on the target server. You should know that this function stops
                   all running goroutines to collect statistics.
  -p  --partitions Print partition statistics of the server.
        --id       Partition id to query.
        --backup   Enable to query backup partitions.
  -d  --dump       Dump stats data in JSON format.
  -D  --dmap       Print DMap statistics.
  -T  --dtopic     Print DTopic statistics.
  -n  --network    Print network statistics.
  -m  --members    List current members of the cluster.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues
`
	_, err := fmt.Fprintf(os.Stdout, msg, runtime.Version())
	if err != nil {
		panic(err)
	}
}

func main() {
	args := &arguments{}
	// No need for timestamp and etc in this function. Just log it.
	log.SetFlags(0)

	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(os.Stdout)
	f.BoolVar(&args.help, "h", false, "")
	f.BoolVar(&args.help, "help", false, "")

	f.BoolVar(&args.version, "version", false, "")
	f.BoolVar(&args.version, "v", false, "")

	f.StringVar(&args.address, "a", defaultAddress, "")
	f.StringVar(&args.address, "addr", defaultAddress, "")

	f.StringVar(&args.timeout, "t", "10ms", "")
	f.StringVar(&args.timeout, "timeout", "10ms", "")

	f.BoolVar(&args.partitions, "p", false, "")
	f.BoolVar(&args.partitions, "partitions", false, "")
	f.IntVar(&args.id, "id", -1, "")
	f.BoolVar(&args.backup, "backup", false, "")

	f.BoolVar(&args.runtime, "r", false, "")
	f.BoolVar(&args.runtime, "runtime", false, "")

	f.BoolVar(&args.dump, "d", false, "")
	f.BoolVar(&args.dump, "dump", false, "")

	f.BoolVar(&args.dmap, "D", false, "")
	f.BoolVar(&args.dmap, "dmap", false, "")

	f.BoolVar(&args.dtopic, "T", false, "")
	f.BoolVar(&args.dtopic, "dtopic", false, "")

	f.BoolVar(&args.network, "n", false, "")
	f.BoolVar(&args.network, "network", false, "")

	f.BoolVar(&args.members, "m", false, "")
	f.BoolVar(&args.members, "members", false, "")

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Fatalf("Failed to parse flags: %v", err)
	}

	switch {
	case args.help:
		usage()
		return
	case args.version:
		_, _ = fmt.Fprintf(os.Stdout,
			"olric-stats %s with runtime %s\n",
			olric.ReleaseVersion, runtime.Version())
	}

	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()

	q, err := query.New(args.address, args.timeout, logger)
	if err != nil {
		logger.Fatalf("olric-stats: %v", err)
	}

	switch {
	case args.dump:
		err = q.Dump()
	case args.runtime:
		err = q.PrintRuntimeStats()
	case args.partitions:
		err = q.PrintPartitionStats(args.id, args.backup)
	case args.members:
		err = q.PrintClusterMembers()
	case args.dmap:
		err = q.PrintDMapStatistics()
	case args.dtopic:
		err = q.PrintDTopicStatistics()
	case args.network:
		err = q.PrintNetworkStatistics()
	default:
		usage()
		return
	}

	if err != nil {
		logger.Fatalf("olric-stats: %v", err)
	}
}
