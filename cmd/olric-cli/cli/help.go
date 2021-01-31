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

package cli

import (
	"fmt"
	"sort"
)

func (c *CLI) helpPut() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Put", Green)))
	c.print(fmt.Sprintf("%s put <key> <value>\n\n", Colorize(">>", Red)))
	c.print("Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.\n")
}

func (c *CLI) helpPutIf() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* PutIf", Green)))
	c.print(fmt.Sprintf("%s putif <key> <value> <flag>\n\n", Colorize(">>", Red)))
	c.print("Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.\n\n")
	c.print("Flag argument currently has two different options:\n\n")
	c.print("  * ifNotFound: Only set the key if it does not already exist. It returns ErrFound if the key already exist.\n")
	c.print("  * ifFound: Only set the key if it already exist.It returns ErrKeyNotFound if the key does not exist.\n")
}

func (c *CLI) helpPutEx() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* PutEx", Green)))
	c.print(fmt.Sprintf("%s putex <key> <value> <ttl>\n\n", Colorize(">>", Red)))
	c.print("PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpPutIfEx() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* PutIfEx", Green)))
	c.print(fmt.Sprintf("%s putifex <key> <value> <ttl> <flags>\n\n", Colorize(">>", Red)))
	c.print("PutIfEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.\n\n")
	c.print("Flag argument currently has two different options:\n\n")
	c.print("  * ifNotFound: Only set the key if it does not already exist. It returns ErrFound if the key already exist.\n")
	c.print("  * ifFound: Only set the key if it already exist.It returns ErrKeyNotFound if the key does not exist.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpGet() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Get", Green)))
	c.print(fmt.Sprintf("%s get <key>\n\n", Colorize(">>", Red)))
	c.print("Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.\n")
}

func (c *CLI) helpExpire() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Expire", Green)))
	c.print(fmt.Sprintf("%s expire <key> <ttl>\n\n", Colorize(">>", Red)))
	c.print("Expire updates the expiry for the given key. It returns 'key not found' if the cluster does not contains the key. It's thread-safe.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpDelete() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Delete", Green)))
	c.print(fmt.Sprintf("%s delete <key>\n\n", Colorize(">>", Red)))
	c.print("Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.\n")
}

func (c *CLI) helpDestroy() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Destroy", Green)))
	c.print(fmt.Sprintf("%s destroy\n\n", Colorize(">>", Red)))
	c.print("Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.\n")
	c.print("So if you call Put/PutEx and Destroy methods concurrently on the cluster, Put/PutEx calls may set new values to the dmap.\n")
}

func (c *CLI) helpIncr() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Incr", Green)))
	c.print(fmt.Sprintf("%s incr <key> <delta>\n\n", Colorize(">>", Red)))
	c.print("Incr atomically increments key by delta. The return value is the new value after being incremented or an error. \"delta\" has to be a valid integer.\n")
}

func (c *CLI) helpDecr() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* Decr", Green)))
	c.print(fmt.Sprintf("%s decr <key> <delta>\n\n", Colorize(">>", Red)))
	c.print("Decr atomically decrements key by delta. The return value is the new value after being decremented or\n")
	c.print("an error. \"delta\" has to be a valid integer.\n")
}

func (c *CLI) helpGetPut() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* GetPut", Green)))
	c.print(fmt.Sprintf("%s getput <key> <value>\n\n", Colorize(">>", Red)))
	c.print("GetPut atomically sets key to value and returns the old value stored at key.\n")
}

func (c *CLI) helpGetEntry() {
	c.print(fmt.Sprintf("%s\n\n", Colorize("* GetEntry", Green)))
	c.print(fmt.Sprintf("%s getentry <key>\n\n", Colorize(">>", Red)))
	c.print("GetEntry gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.\n")
}

func (c *CLI) help(cmd string) error {
	var commands = map[string]func(){
		"put":      c.helpPut,
		"putif":    c.helpPutIf,
		"putex":    c.helpPutEx,
		"putIfEx":  c.helpPutIfEx,
		"get":      c.helpGet,
		"expire":   c.helpExpire,
		"delete":   c.helpDelete,
		"destroy":  c.helpDestroy,
		"incr":     c.helpIncr,
		"decr":     c.helpDecr,
		"getput":   c.helpGetPut,
		"getentry": c.helpGetEntry,
	}

	if cmd != "" {
		if f, ok := commands[cmd]; ok {
			f()
			return nil
		}
		return fmt.Errorf("invalid command: %s", cmd)
	}

	// Print the help text in order.
	var keys []string
	for key, _ := range commands {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	c.print("Available commands:\n\n")
	for _, key := range keys {
		c.print("* " + key + "\n")
	}
	c.print("\nType \"help <command-name>\" to learn how to use.\n")
	return nil
}
