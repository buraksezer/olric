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

/*Package cli is the Olric command line interface, a simple program that allows
to send commands to Olric, and read the replies sent by the server, directly from
the terminal.*/
package cli

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric/client"
	_serializer "github.com/buraksezer/olric/serializer"
	"github.com/chzyer/readline"
)

// CLI defines the command line client for Olric.
type CLI struct {
	addr   string
	client *client.Client
	output io.Writer
}

// New returns a new CLI instance.
func New(addr string, serializer, timeout string, logger *log.Logger) (*CLI, error) {
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
		Addrs:       []string{addr},
		Serializer:  s,
		DialTimeout: dt,
	}
	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}
	if err = c.Ping(addr); err != nil {
		return nil, err
	}
	return &CLI{
		addr:   addr,
		client: c,
	}, nil
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),
	readline.PcItem("use"),
	readline.PcItem("put"),
	readline.PcItem("putex"),
	readline.PcItem("get"),
	readline.PcItem("delete"),
	readline.PcItem("destroy"),
	readline.PcItem("incr"),
	readline.PcItem("decr"),
)

func help() {
	fmt.Println("help text")
}

func extractKey(str string) (string, error) {
	res := regexp.MustCompile(`"([^"]*)"`).FindStringSubmatch(str)
	if len(res) < 2 {
		return "", fmt.Errorf("invalid command")
	}
	return res[1], nil
}

func (c *CLI) print(msg string) {
	io.WriteString(c.output, msg)
}

func parseLine(tmp string) (string, string, error) {
	tmp = strings.TrimSpace(tmp)
	var key, value string
	if strings.HasPrefix(tmp, "\"") {
		key, err := extractKey(tmp)
		if err != nil {
			return "", "", err
		}
		rval := strings.Split(tmp, fmt.Sprintf("\"%s\"", key))
		if len(rval) < 1 {
			return "", "", fmt.Errorf("invalid command")
		}
		value = strings.TrimSpace(rval[0])
	} else {
		res := strings.SplitN(tmp, " ", 2)
		if len(res) < 2 {
			return "", "", fmt.Errorf("invalid command")
		}
		key, value = res[0], res[1]
	}
	return key, value, nil
}

func (c *CLI) evaluate(dmap, line string) error {
	var err error
	line = strings.TrimSpace(line)

	dm := c.client.NewDMap(dmap)
	switch {
	case strings.HasPrefix(line, "put "):
		tmp := strings.TrimLeft(line, "put ")
		tmp = strings.TrimSpace(tmp)
		key, value, err := parseLine(tmp)
		if err != nil {
			return err
		}
		if err := dm.Put(key, value); err != nil {
			return err
		}
	case strings.HasPrefix(line, "putex "):
		tmp := strings.TrimLeft(line, "putex ")
		tmp = strings.TrimSpace(tmp)
		var key, value, tval, tt string
		if strings.HasPrefix(tmp, "\"") {
			key, err = extractKey(tmp)
			if err != nil {
				return err
			}
			rval := strings.Split(tmp, fmt.Sprintf("\"%s\"", key))
			if len(rval) < 1 {
				return fmt.Errorf("invalid command: %s\n", line)
			}
			tval = rval[0]
		} else {
			res := strings.SplitN(tmp, " ", 2)
			if len(res) < 2 {
				return fmt.Errorf("invalid command: %s\n", line)
			}
			key, tval = res[0], res[1]
		}

		tval = strings.TrimSpace(tval)
		res := strings.SplitN(tval, " ", 2)
		if len(res) < 2 {
			return fmt.Errorf("invalid command: %s\n", line)
		}
		tt, value = res[0], res[1]
		ttl, err := time.ParseDuration(tt)
		if err != nil {
			return err
		}

		if err := dm.PutEx(key, value, ttl); err != nil {
			return err
		}
	case strings.HasPrefix(line, "get "):
		key := strings.TrimLeft(line, "get ")
		if strings.HasPrefix(key, "\"") {
			key, err = extractKey(key)
			if err != nil {
				return err
			}
		}
		value, err := dm.Get(key)
		if err != nil {
			return err
		}
		c.print(fmt.Sprintf("%v\n", value))
	case strings.HasPrefix(line, "delete "):
		key := strings.TrimLeft(line, "delete ")
		if strings.HasPrefix(key, "\"") {
			key, err = extractKey(key)
			if err != nil {
				return err
			}
		}
		err := dm.Delete(key)
		if err != nil {
			return err
		}
	case strings.HasPrefix(line, "destroy"):
		err := dm.Destroy()
		if err != nil {
			return err
		}
	case strings.HasPrefix(line, "incr "):
		tmp := strings.TrimLeft(line, "incr ")
		key, value, err := parseLine(tmp)
		if err != nil {
			return err
		}
		delta, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid delta: %s", err)
		}
		current, err := dm.Incr(key, delta)
		if err != nil {
			return err
		}
		c.print(fmt.Sprintf("%d\n", current))
	case strings.HasPrefix(line, "decr "):
		tmp := strings.TrimLeft(line, "decr ")
		key, value, err := parseLine(tmp)
		if err != nil {
			return fmt.Errorf("invalid delta: %s", err)
		}
		delta, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid delta: %s", err)
		}
		current, err := dm.Decr(key, delta)
		if err != nil {
			return err
		}
		c.print(fmt.Sprintf("%d\n", current))
	default:
		return fmt.Errorf("invalid command")
	}
	return nil
}

// RunCommand runs the given command on dmap.
func (c *CLI) RunCommand(dmap, cmd string) error {
	c.output = os.Stderr
	return c.evaluate(dmap, cmd)
}

// WaitForCommand waits for new input from stdin.
func (c *CLI) WaitForCommand(dmap string) error {
	var historyFile string
	home := os.Getenv("HOME")
	if home != "" {
		historyFile = path.Join(home, ".olric-cli_history")
	} else {
		c.print("[WARN] $HOME is empty.\n")
	}
	prompt := fmt.Sprintf("[%s] \033[31m»\033[0m ", c.addr)
	l, err := readline.NewEx(&readline.Config{
		Prompt:          prompt,
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
	})
	if err != nil {
		return err
	}
	defer func() {
		err = l.Close()
		if err != nil {
			c.print(fmt.Sprintf("Failed to close readline: %v\n", err))
		}
	}()
	c.output = l.Stderr()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if len(line) == 0 {
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

		switch {
		case strings.HasPrefix(line, "exit"):
			c.print("Quit\n")
			return nil
		case strings.HasPrefix(line, "use "):
			tmp := strings.Split(line, "use")[1]
			dmap = strings.Trim(tmp, " ")
			if strings.HasPrefix(dmap, "\"") {
				dmap, err = extractKey(dmap)
				if err != nil {
					c.print(fmt.Sprintf("Failed to get database name: %s: %v\n", line, err))
				}
			}
			c.print(fmt.Sprintf("use %s\n", dmap))
		case strings.HasPrefix(line, "help"):
			help()
		default:
			if len(dmap) == 0 {
				c.print("Call 'use <database-name>' command before accessing the database.\n")
			} else {
				if err := c.evaluate(dmap, line); err != nil {
					c.print(fmt.Sprintf("Failed to call %s on %s: %v\n", line, dmap, err))
				}
			}
		}
	}
	return nil
}
