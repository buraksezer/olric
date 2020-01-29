// Copyright 2018-2019 Burak Sezer
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

// Server implementation for Olric. Olricd basically manages configuration for you.

package main

import (
	"flag"
	"github.com/sean-/seed"
	"io/ioutil"
	"log"
	"os"
	"runtime"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olricd/server"
)

var usage = `Usage: 
  olricd [flags] ...

Flags:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.

  -c -config                    
      Sets configuration file path. Default is olricd.yaml in the current folder.
      Set OLRICD_CONFIG to overwrite it.

The Go runtime version %s
Report bugs to https://github.com/buraksezer/olric/issues`

var (
	cpath       string
	showHelp    bool
	showVersion bool
)

func main() {
	// No need for timestamp and etc in this function. Just log it.
	log.SetFlags(0)

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
		log.Fatalf("Failed to parse flags: %v", err)
	}

	if showVersion {
		log.Printf("olricd %s with runtime %s\n", olric.ReleaseVersion, runtime.Version())
		return
	} else if showHelp {
		log.Printf(usage, runtime.Version())
		return
	}

	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()

	c, err := server.NewConfig(cpath)
	if err != nil {
		log.Fatalf("Failed to read or parse configuration file: %v", err)
	}
	s, err := server.New(c)
	if err != nil {
		log.Fatalf("Failed to create a new olricd instance:\n%v", err)
	}

	if err = s.Start(); err != nil {
		log.Fatalf("Olric quits prematurely:\n%v", err)
	}
	log.Print("Quit!")
}
