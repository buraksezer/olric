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
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/buraksezer/olricdb"
	"github.com/buraksezer/olricdb/cmd/olricdb-cli/cli"
)

var usage = `olricdb-cli is a CLI interface for OlricDB

Usage: 
  olricdb-cli [flags] ...

Flags:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.
  
  -k, -insecure      
      Allow insecure server connections when using SSL

  -u -uri
      Server URI.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olricdb/issues`

var (
	showHelp    bool
	showVersion bool
	insecure    bool
	uri         string
	timeout     string
	dialTimeout string
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
	f.BoolVar(&showVersion, "version", false, "")
	f.BoolVar(&showVersion, "v", false, "")
	f.BoolVar(&insecure, "k", false, "")
	f.BoolVar(&insecure, "insecure", false, "")
	f.StringVar(&timeout, "timeout", "10s", "")
	f.StringVar(&dialTimeout, "dialTimeout", "10s", "")
	f.StringVar(&uri, "uri", "https://127.0.0.1:3320", "")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Printf("Failed to parse flags: %v", err)
		os.Exit(1)
	}

	if showVersion {
		fmt.Printf("olricdb-cli %s with runtime %s\n", olricdb.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		msg := fmt.Sprintf(usage, runtime.Version())
		fmt.Println(msg)
		return
	}

	c, err := cli.New(uri, insecure, dialTimeout, timeout)
	if err != nil {
		fmt.Printf("[ERROR] Failed to create olricdb-cli instance: %v", err)
		os.Exit(1)
	}
	if err := c.Start(); err != nil {
		fmt.Printf("[ERROR] olricdb-cli has returned an error: %v", err)
		os.Exit(1)
	}
}
