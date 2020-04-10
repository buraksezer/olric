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
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric/client"
	_serializer "github.com/buraksezer/olric/serializer"
	"github.com/chzyer/readline"
	"github.com/sean-/seed"
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
		Addrs:       []string{addr},
		Serializer:  s,
		DialTimeout: dt,
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

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),
	readline.PcItem("use"),
	readline.PcItem("put"),
	readline.PcItem("putex"),
	readline.PcItem("get"),
	readline.PcItem("delete"),
	readline.PcItem("destroy"),
	readline.PcItem("getput"),
	readline.PcItem("incr"),
	readline.PcItem("decr"),
	readline.PcItem("expire"),
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

func trimPrefix(s, prefix string) string {
	res := strings.TrimPrefix(s, prefix)
	return strings.TrimSpace(res)
}

func parseLine(tmp string) (key, value string, err error) {
	tmp = strings.TrimSpace(tmp)
	if strings.HasPrefix(tmp, "\"") {
		key, err = extractKey(tmp)
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

func (c *CLI) evalPut(dm *client.DMap, line string) error {
	tmp := trimPrefix(line, "put ")
	key, value, err := parseLine(tmp)
	if err != nil {
		return err
	}
	return dm.Put(key, value)
}

func (c *CLI) evalPutex(dm *client.DMap, line string) error {
	var err error
	tmp := trimPrefix(line, "putex ")
	var key, value, tval, ttlRaw string
	if strings.HasPrefix(tmp, "\"") {
		key, err = extractKey(tmp)
		if err != nil {
			return err
		}
		rval := strings.Split(tmp, fmt.Sprintf("\"%s\"", key))
		if len(rval) < 1 {
			return fmt.Errorf("invalid command: %s", line)
		}
		tval = rval[0]
	} else {
		res := strings.SplitN(tmp, " ", 2)
		if len(res) < 2 {
			return fmt.Errorf("invalid command: %s", line)
		}
		key, tval = res[0], res[1]
	}

	tval = strings.TrimSpace(tval)
	res := strings.SplitN(tval, " ", 2)
	if len(res) < 2 {
		return fmt.Errorf("invalid command: %s", line)
	}
	ttlRaw, value = res[0], res[1]
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return err
	}

	return dm.PutEx(key, value, ttl)
}

func (c *CLI) evalGet(dm *client.DMap, line string) error {
	var err error
	key := trimPrefix(line, "get ")
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
	return nil
}

func (c *CLI) evalDelete(dm *client.DMap, line string) error {
	var err error
	key := trimPrefix(line, "delete ")
	if strings.HasPrefix(key, "\"") {
		key, err = extractKey(key)
		if err != nil {
			return err
		}
	}
	return dm.Delete(key)
}

func (c *CLI) evalIncr(dm *client.DMap, line string) error {
	tmp := trimPrefix(line, "incr ")
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
	return nil
}

func (c *CLI) evalDestroy(dm *client.DMap) error {
	return dm.Destroy()
}

func (c *CLI) evalDecr(dm *client.DMap, line string) error {
	tmp := trimPrefix(line, "decr ")
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
	return nil
}

func (c *CLI) evalExpire(dm *client.DMap, line string) error {
	tmp := trimPrefix(line, "expire ")
	key, ttlRaw, err := parseLine(tmp)
	if err != nil {
		return err
	}
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return err
	}
	return dm.Expire(key, ttl)
}

func (c *CLI) evalGetPut(dm *client.DMap, line string) error {
	tmp := trimPrefix(line, "getput ")
	key, value, err := parseLine(tmp)
	if err != nil {
		return err
	}

	current, err := dm.GetPut(key, value)
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%v\n", current))
	return nil
}

func (c *CLI) evaluate(dmap, line string) error {
	line = strings.TrimSpace(line)

	dm := c.client.NewDMap(dmap)
	switch {
	case strings.HasPrefix(line, "put "):
		return c.evalPut(dm, line)
	case strings.HasPrefix(line, "putex "):
		return c.evalPutex(dm, line)
	case strings.HasPrefix(line, "get "):
		return c.evalGet(dm, line)
	case strings.HasPrefix(line, "delete "):
		return c.evalDelete(dm, line)
	case strings.HasPrefix(line, "destroy"):
		return c.evalDestroy(dm)
	case strings.HasPrefix(line, "incr "):
		return c.evalIncr(dm, line)
	case strings.HasPrefix(line, "decr "):
		return c.evalDecr(dm, line)
	case strings.HasPrefix(line, "expire "):
		return c.evalExpire(dm, line)
	case strings.HasPrefix(line, "getput "):
		return c.evalGetPut(dm, line)
	default:
		return fmt.Errorf("invalid command")
	}
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
					c.error(fmt.Sprintf("Failed to get DMap name: %s: %v\n", line, err))
				}
			}
			c.print(fmt.Sprintf("use %s\n", dmap))
		case strings.HasPrefix(line, "help"):
			help()
		default:
			if dmap == "" {
				c.print("Call 'use <dmap-name>' command before accessing a DMap.\n")
				continue
			}
			if err := c.evaluate(dmap, line); err != nil {
				c.error(fmt.Sprintf("Failed to call %s on %s: %v\n", line, dmap, err))
			}
		}
	}
	return nil
}
