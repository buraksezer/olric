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
	c.print("# Put #\n\n")
	c.print(">> put <key> <value>\n\n")
	c.print("Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.\n")
}

func (c *CLI) helpPutIf() {
	c.print("# PutIf #\n\n")
	c.print(">> putif <key> <value> <flag>\n\n")
	c.print("Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.\n\n")
	c.print("Flag argument currently has two different options:\n\n")
	c.print("  * ifNotFound: Only set the key if it does not already exist. It returns ErrFound if the key already exist.\n")
	c.print("  * ifFound: Only set the key if it already exist.It returns ErrKeyNotFound if the key does not exist.\n")
}

func (c *CLI) helpPutEx() {
	c.print("# PutEx #\n\n")
	c.print(">> putex <key> <value> <ttl>\n\n")
	c.print("PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpPutIfEx() {
	c.print("# PutIfEx #\n\n")
	c.print(">> putifex <key> <value> <ttl> <flags>\n\n")
	c.print("PutIfEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.\n\n")
	c.print("Flag argument currently has two different options:\n\n")
	c.print("  * ifNotFound: Only set the key if it does not already exist. It returns ErrFound if the key already exist.\n")
	c.print("  * ifFound: Only set the key if it already exist.It returns ErrKeyNotFound if the key does not exist.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpGet() {
	c.print("# Get #\n\n")
	c.print(">> get <key>\n\n")
	c.print("Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.\n")
}

func (c *CLI) helpExpire() {
	c.print("# Expire #\n\n")
	c.print(">> expire <key> <ttl>\n\n")
	c.print("Expire updates the expiry for the given key. It returns 'key not found' if the cluster does not contains the key. It's thread-safe.\n\n")
	c.print("\"ttl\" is a duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\".\n")
	c.print("Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\n")
}

func (c *CLI) helpDelete() {
	c.print("# Delete #\n\n")
	c.print(">> delete <key>\n\n")
	c.print("Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.\n")
}

func (c *CLI) helpDestroy() {
	c.print("# Destroy #\n\n")
	c.print(">> destroy\n\n")
	c.print("Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.\n")
	c.print("So if you call Put/PutEx and Destroy methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.\n")
}

func (c *CLI) helpIncr() {
	c.print("# Incr #\n\n")
	c.print(">> incr <key> <delta>\n\n")
	c.print("Incr atomically increments key by delta. The return value is the new value after being incremented or an error. \"delta\" has to be a valid integer.\n")
}

func (c *CLI) helpDecr() {
	c.print("# Decr #\n\n")
	c.print(">> decr <key> <delta>\n\n")
	c.print("Decr atomically decrements key by delta. The return value is the new value after being decremented or an error. \"delta\" has to be a valid integer.\n")
}

func (c *CLI) helpGetPut() {
	c.print("# GetPut #\n\n")
	c.print(">> getput <key> <value>\n\n")
	c.print("GetPut atomically sets key to value and returns the old value stored at key.\n")
}

func (c *CLI) help(cmd string) error {
	var commands = map[string]func(){
		"put":     c.helpPut,
		"putif":   c.helpPutIf,
		"putex":   c.helpPutEx,
		"putIfEx": c.helpPutIfEx,
		"get":     c.helpGet,
		"expire":  c.helpExpire,
		"delete":  c.helpDelete,
		"destroy": c.helpDestroy,
		"incr":    c.helpIncr,
		"decr":    c.helpDecr,
		"getput":  c.helpGetPut,
	}

	if cmd != "" {
		if f, ok := commands[cmd]; ok {
			f()
			return nil
		}
		return fmt.Errorf("invalid command: %s", cmd)
	}

	// Print the help text in order.
	keys := []string{}
	for key, _ := range commands {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	c.print("Available commands:\n\n")
	for _, key := range keys {
		commands[key]()
		c.print("\n")
	}
	return nil
}
