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

/*Package cli is the Olric command line interface, a simple program that allows
to send commands to Olric, and read the replies sent by the server, directly from
the terminal.*/
package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/config"
	_serializer "github.com/buraksezer/olric/serializer"
	"github.com/chzyer/readline"
	"github.com/sean-/seed"
)

var errInvalidCommand = errors.New("invalid command")

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),
	readline.PcItem("use"),
	readline.PcItem("put"),
	readline.PcItem("putex"),
	readline.PcItem("putif"),
	readline.PcItem("putifex"),
	readline.PcItem("get"),
	readline.PcItem("delete"),
	readline.PcItem("destroy"),
	readline.PcItem("getput"),
	readline.PcItem("incr"),
	readline.PcItem("decr"),
	readline.PcItem("expire"),
)

// CLI defines the command line client for Olric.
type CLI struct {
	addr   string
	client *client.Client
	stdout io.Writer
	stderr io.Writer
}

// New returns a new CLI instance.
func New(addr, serializer, timeout string) (*CLI, error) {
	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var s _serializer.Serializer
	if serializer == "json" {
		s = _serializer.NewJSONSerializer()
	} else if serializer == "msgpack" {
		s = _serializer.NewMsgpackSerializer()
	} else if serializer == "gob" {
		s = _serializer.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", serializer)
	}
	dt, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}

	cc := &client.Config{
		Servers:    []string{addr},
		Serializer: s,
		Client: &config.Client{
			DialTimeout: dt,
		},
	}

	// MustInit provides guaranteed secure seeding.  If `/dev/urandom` is not
	// available, MustInit will panic() with an error indicating why reading from
	// `/dev/urandom` failed.  MustInit() will upgrade the seed if for some reason a
	// call to Init() failed in the past.
	seed.MustInit()

	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}
	// Check the connection
	if err := c.Ping(addr); err != nil {
		return nil, err
	}

	return &CLI{
		addr:   addr,
		client: c,
	}, nil
}

func (c *CLI) print(msg string) {
	if c.stdout == nil {
		_, _ = io.WriteString(os.Stdout, msg)
		return
	}
	_, _ = io.WriteString(c.stdout, msg)
}

func (c *CLI) error(msg string) {
	if c.stderr == nil {
		_, _ = io.WriteString(os.Stderr, msg)
		return
	}
	_, _ = io.WriteString(c.stderr, msg)
}

// RunCommand runs the given command on dmap.
func (c *CLI) RunCommand(dmap, cmd string) error {
	c.stdout = os.Stdout
	return c.evaluate(dmap, cmd)
}

func (c *CLI) buildRepl() (*readline.Instance, error) {
	var historyFile string
	home := os.Getenv("HOME")
	if home != "" {
		historyFile = path.Join(home, ".olric-cli_history")
	} else {
		c.error("[WARN] $HOME is empty.\n")
	}
	prompt := fmt.Sprintf("[%s] \033[31m»\033[0m ", c.addr)
	reader, err := readline.NewEx(&readline.Config{
		Prompt:          prompt,
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
	})
	if err != nil {
		return nil, err
	}
	c.stdout = reader.Stdout()
	c.stderr = reader.Stderr()
	return reader, nil
}

// WaitForCommand waits for new input from stdin.
func (c *CLI) WaitForCommand(dmap string) error {
	reader, err := c.buildRepl()
	if err != nil {
		return err
	}

	defer func() {
		err = reader.Close()
		if err != nil {
			c.error(fmt.Sprintf("Failed to close readline: %v\n", err))
		}
	}()

	c.print("Type \"help\" or \"help <command-name>\" to learn how to use.\n")
	for {
		line, err := reader.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if line == "" {
					break
				} else {
					continue
				}
			} else if err == io.EOF {
				break
			} else {
				return err
			}
		}
		fields := strings.Fields(strings.Trim(line, " "))
		if len(fields) == 0 {
			// no command given
			continue
		}
		cmd := fields[0]
		switch {
		case cmd == "exit":
			c.print("Quit\n")
			return nil
		case cmd == "use":
			dmap = strings.Join(fields[1:], " ")
			c.print(fmt.Sprintf("use %s\n", dmap))
		case cmd == "help":
			var helpCmd string
			if len(fields) > 1 {
				helpCmd = fields[1]
			}
			if err := c.help(helpCmd); err != nil {
				c.error(fmt.Sprintf("%s\n", err))
			}
		default:
			if dmap == "" {
				c.print("Call 'use <dmap-name>' command before accessing a dmap.\n")
				continue
			}
			if err := c.evaluate(dmap, line); err != nil {
				c.error(fmt.Sprintf("Failed to call %s on %s: %v\n", line, dmap, err))
			}
		}
	}
	return nil
}
