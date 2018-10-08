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

	"github.com/buraksezer/olricdb/cmd/olricd/server"
)

var usage = `olricd is the default standalone server for OlricDB

Usage: 
  olricd [flags] ...

Flags:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.

  -c -config                    
      Sets configuration file path. Default is /etc/olricd.toml
      Set OLRICD_CONFIG to overwrite it.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olricdb/issues`

const version = "0.1"

var (
	cpath       string
	showHelp    bool
	showVersion bool
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
	f.StringVar(&cpath, "config", server.DefaultConfigFile, "")
	f.StringVar(&cpath, "c", server.DefaultConfigFile, "")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Printf("Failed to parse flags: %v", err)
		os.Exit(1)
	}

	if showVersion {
		fmt.Printf("olricd %s with runtime %s\n", version, runtime.Version())
		return
	} else if showHelp {
		msg := fmt.Sprintf(usage, runtime.Version())
		fmt.Println(msg)
		return
	}

	c, err := server.NewConfig(cpath)
	if err != nil {
		log.Printf("Failed to parse config file: %v", err)
		os.Exit(1)
	}
	s, err := server.New(c)
	if err != nil {
		log.Printf("Failed to create a new olricd instance: %v", err)
		os.Exit(1)
	}

	if err = s.Start(); err != nil {
		log.Printf("olricd returned an error: %v", err)
		os.Exit(1)
	}
	log.Print("Quit!")
}
