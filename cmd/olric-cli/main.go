// Copyright 2018-2020 Burak Sezer
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

const (
	defaultSerializer string = "gob"
	defaultAddr       string = "127.0.0.1:3320"
)

var usage = `Usage: 
  olric-cli [flags] [commands] ...

Flags:
  -h -help                      
      Shows this screen.
  -v -version                   
      Shows version information.
  -d -dmap
      DMap to access.
  -c -command
      Command to run.
  -s -serializer
      Specifies serialization format. 
      Available formats: gob, json, msgpack. Default: %s
  -a -addr
      Server URI. Default: %s
  -t timeout
      Specifies a time limit for requests and dial made by Olric client

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues`

var (
	showHelp    bool
	showVersion bool
	dmap        string
	command     string
	addr        string
	timeout     string
	serializer  string
)

func main() {
	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.BoolVar(&showHelp, "h", false, "")
	f.BoolVar(&showHelp, "help", false, "")

	f.BoolVar(&showVersion, "v", false, "")
	f.BoolVar(&showVersion, "version", false, "")

	f.StringVar(&command, "c", "", "")
	f.StringVar(&command, "command", "", "")

	f.StringVar(&dmap, "d", "", "")
	f.StringVar(&dmap, "dmap", "", "")

	f.StringVar(&timeout, "t", "10s", "")
	f.StringVar(&timeout, "timeout", "10s", "")

	f.StringVar(&addr, "a", defaultAddr, "")
	f.StringVar(&addr, "addr", defaultAddr, "")

	f.StringVar(&serializer, "s", defaultSerializer, "")
	f.StringVar(&serializer, "serializer", defaultSerializer, "")

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if err := f.Parse(os.Args[1:]); err != nil {
		logger.Fatalf("Failed to parse flags: %v", err)
	}

	if showVersion {
		logger.Printf("olric-cli %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		logger.Println(fmt.Sprintf(usage, defaultSerializer, defaultAddr, runtime.Version()))
		return
	}

	c, err := cli.New(addr, serializer, timeout)
	if err != nil {
		logger.Fatalf("Failed to create olric-cli instance: %v\n", err)
	}

	if command != "" {
		if err := c.RunCommand(dmap, command); err != nil {
			logger.Fatalf("olric-cli has returned an error: %v\n", err)
		}
		return
	}

	if err := c.WaitForCommand(dmap); err != nil {
		logger.Fatalf("olric-cli has returned an error: %v\n", err)
	}
}
