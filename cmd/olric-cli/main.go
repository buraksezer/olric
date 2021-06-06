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

// Command line interface for Olric
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-cli/cli"
)

func usage() {
	var msg = `Usage: olric-cli [options] [command] ...

Send commands to Olric cluster, and read the replies sent by the server, directly 
from the terminal.

Options:
  -h, --help       Print this message and exit.
  -v, --version    Print the version number and exit.
  -a  --address    Network address of the server in <host:port> format.
                   Default: 127.0.0.1:3320
  -t  --timeout    Set time limit for requests and dial made by the client.
                   Default: 10ms
  -d  --dmap       DMap to access.
  -c  --command    Command to run.
  -s  --serializer Serialization format. Built-in: gob, json, msgpack.
                   Default: gob

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues
`
	_, err := fmt.Fprintf(os.Stdout, msg, runtime.Version())
	if err != nil {
		panic(err)
	}
}

type arguments struct {
	help       bool
	version    bool
	dmap       string
	command    string
	address    string
	timeout    string
	serializer string
}

func main() {
	args := arguments{}

	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	f.BoolVar(&args.help, "h", false, "")
	f.BoolVar(&args.help, "help", false, "")

	f.BoolVar(&args.version, "v", false, "")
	f.BoolVar(&args.version, "version", false, "")

	f.StringVar(&args.command, "c", "", "")
	f.StringVar(&args.command, "command", "", "")

	f.StringVar(&args.dmap, "d", "", "")
	f.StringVar(&args.dmap, "dmap", "", "")

	f.StringVar(&args.timeout, "t", "10s", "")
	f.StringVar(&args.timeout, "timeout", "10s", "")

	f.StringVar(&args.address, "a", "127.0.0.1:3320", "")
	f.StringVar(&args.address, "address", "127.0.0.1:3320", "")

	f.StringVar(&args.serializer, "s", "gob", "")
	f.StringVar(&args.serializer, "serializer", "gob", "")

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		logger.Fatalf("Failed to parse flags: %v", err)
	}

	if args.version {
		logger.Printf("olric-cli %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if args.help {
		usage()
		return
	}

	c, err := cli.New(args.address, args.serializer, args.timeout)
	if err != nil {
		logger.Fatalf("olric-cli: %v\n", err)
	}

	if args.command != "" {
		if err := c.RunCommand(args.dmap, args.command); err != nil {
			logger.Fatalf("olric-cli: %v\n", err)
		}
		return
	}

	if err := c.WaitForCommand(args.dmap); err != nil {
		logger.Fatalf("olric-cli has returned an error: %v\n", err)
	}
}
