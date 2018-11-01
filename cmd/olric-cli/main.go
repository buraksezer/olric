// Copyright 2018 Burak Sezer
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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-cli/cli"
)

const (
	defaultSerializer string = "gob"
	defaultAddr       string = "127.0.0.1:3320"
)

var usage = `olric-cli is a CLI interface for Olric

Usage: 
  olric-cli [flags] ...

Flags:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.
  
  -k -insecure      
      Allow insecure server connections when using SSL
  
  -s -serializer
      Specifies serialization format. Available formats: gob, json, msgpack. Default: %s

  -a -addr
      Server URI. Default: %s

  -t timeout
      Specifies a time limit for requests and dial made by Olric client

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues`

var (
	showHelp    bool
	showVersion bool
	insecure    bool
	addr        string
	timeout     string
	serializer  string
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.BoolVar(&showHelp, "h", false, "")
	f.BoolVar(&showHelp, "help", false, "")

	f.BoolVar(&showVersion, "v", false, "")
	f.BoolVar(&showVersion, "version", false, "")

	f.BoolVar(&insecure, "k", false, "")
	f.BoolVar(&insecure, "insecure", false, "")

	f.StringVar(&timeout, "t", "10s", "")
	f.StringVar(&timeout, "timeout", "10s", "")

	f.StringVar(&addr, "a", defaultAddr, "")
	f.StringVar(&addr, "addr", defaultAddr, "")

	f.StringVar(&serializer, "s", defaultSerializer, "")
	f.StringVar(&serializer, "serializer", defaultSerializer, "")

	if err := f.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v", err)
		os.Exit(1)
	}

	if showVersion {
		fmt.Printf("olric-cli %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		msg := fmt.Sprintf(usage, defaultSerializer, defaultAddr, runtime.Version())
		fmt.Println(msg)
		return
	}

	c, err := cli.New(addr, insecure, serializer, timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to create olric-cli instance: %v", err)
		os.Exit(1)
	}
	if err := c.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] olric-cli has returned an error: %v", err)
		os.Exit(1)
	}
}
