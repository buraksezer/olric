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

/*Package cli is the Olric command line interface, a simple program that allows
to send commands to Olric, and read the replies sent by the server, directly from
the terminal.*/
package cli

import (
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/client"
	"github.com/chzyer/readline"
)

// CLI defines the command line client for Olric.
type CLI struct {
	addr   string
	client *client.Client
	output io.Writer
}

// New returns a new CLI instance.
func New(addr string, insecureSkipVerify bool, serializer, timeout string) (*CLI, error) {
	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var s olric.Serializer
	if serializer == "json" {
		s = olric.NewJSONSerializer()
	} else if serializer == "msgpack" {
		s = olric.NewMsgpackSerializer()
	} else if serializer == "gob" {
		s = olric.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", serializer)
	}

	cc := &client.Config{
		Addrs: []string{addr},
	}
	c, err := client.New(cc, s)
	if err != nil {
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

// Start waits for new input from stdin.
func (c *CLI) Start() error {
	var historyFile string
	home := os.Getenv("HOME")
	if home != "" {
		historyFile = path.Join(home, ".olriccli_history")
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
			c.print(fmt.Sprintf("[ERROR] Failed to close readline: %v\n", err))
		}
	}()
	c.output = l.Stderr()

	var dmap string
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

		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "exit"):
			return nil
		case strings.HasPrefix(line, "use "):
			tmp := strings.Split(line, "use")[1]
			dmap = strings.Trim(tmp, " ")
			if strings.HasPrefix(dmap, "\"") {
				dmap, err = extractKey(dmap)
				if err != nil {
					c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
					continue
				}
			}
			c.print(fmt.Sprintf("use %s\n", dmap))
			continue
		}

		dm := c.client.NewDMap(dmap)
		if len(dmap) == 0 {
			c.print("[ERROR] Call 'use' command before accessing database.\n")
			continue
		}

		switch {
		case strings.HasPrefix(line, "put "):
			tmp := strings.TrimLeft(line, "put ")
			tmp = strings.TrimSpace(tmp)
			key, value, err := parseLine(tmp)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
				continue
			}
			if err := dm.Put(key, value); err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Put on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "putex "):
			tmp := strings.TrimLeft(line, "putex ")
			tmp = strings.TrimSpace(tmp)
			var key, value, tval, tt string
			if strings.HasPrefix(tmp, "\"") {
				key, err = extractKey(tmp)
				if err != nil {
					c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
					continue
				}
				rval := strings.Split(tmp, fmt.Sprintf("\"%s\"", key))
				if len(rval) < 1 {
					c.print(fmt.Sprintf("[ERROR] invalid command: %s\n", line))
					continue
				}
				tval = rval[0]
			} else {
				res := strings.SplitN(tmp, " ", 2)
				if len(res) < 2 {
					c.print(fmt.Sprintf("[ERROR] invalid command: %s\n", line))
					continue
				}
				key, tval = res[0], res[1]
			}

			tval = strings.TrimSpace(tval)
			res := strings.SplitN(tval, " ", 2)
			if len(res) < 2 {
				c.print(fmt.Sprintf("[ERROR] invalid command: %s\n", line))
				continue
			}
			tt, value = res[0], res[1]
			ttl, err := time.ParseDuration(tt)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
				continue
			}

			if err := dm.PutEx(key, value, ttl); err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call PutEx on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "get "):
			key := strings.TrimLeft(line, "get ")
			if strings.HasPrefix(key, "\"") {
				key, err = extractKey(key)
				if err != nil {
					c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
					continue
				}
			}
			value, err := dm.Get(key)
			if err == olric.ErrKeyNotFound {
				c.print("nil\n")
				continue
			}
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Get on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print(fmt.Sprintf("%v\n", value))
		case strings.HasPrefix(line, "delete "):
			key := strings.TrimLeft(line, "delete ")
			if strings.HasPrefix(key, "\"") {
				key, err = extractKey(key)
				if err != nil {
					c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
					continue
				}
			}
			err := dm.Delete(key)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Delete on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "destroy"):
			err := dm.Destroy()
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Destroy on %s: %v\n", dmap, err))
				continue
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "incr "):
			tmp := strings.TrimLeft(line, "incr ")
			key, value, err := parseLine(tmp)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
				continue
			}

			delta, err := strconv.Atoi(value)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] invalid delta: %s\n", value))
				continue
			}
			current, err := dm.Incr(key, delta)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Incr on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print(fmt.Sprintf("%d\n", current))
		case strings.HasPrefix(line, "decr "):
			tmp := strings.TrimLeft(line, "decr ")
			key, value, err := parseLine(tmp)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] %v: %s\n", err, line))
				continue
			}

			delta, err := strconv.Atoi(value)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] invalid delta: %s\n", value))
				continue
			}
			current, err := dm.Decr(key, delta)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Decr on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print(fmt.Sprintf("%d\n", current))
		default:
			c.print("[ERROR] Invalid command. Available commands: put, putex, get, delete, destroy and exit.\n")
		}
	}
	return nil
}
