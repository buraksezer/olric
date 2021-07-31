// Copyright 2018-2021 Burak Sezer
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/client"
)

const (
	cmdPut      string = "put"
	cmdPutEx    string = "putex"
	cmdGet      string = "get"
	cmdDelete   string = "delete"
	cmdDestroy  string = "destroy"
	cmdExpire   string = "expire"
	cmdPutIf    string = "putif"
	cmdPutIfEx  string = "putifex"
	cmdIncr     string = "incr"
	cmdDecr     string = "decr"
	cmdGetPut   string = "getput"
	cmdGetEntry string = "getentry"
)

func (c *CLI) evalGetEntry(dm *client.DMap, fields []string) error {
	key := strings.Join(fields, " ")
	entry, err := dm.GetEntry(key)
	if err != nil {
		return err
	}
	text, err := json.MarshalIndent(entry, "", "\t")
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%s\n", text))
	return nil
}

func (c *CLI) evalPut(dm *client.DMap, fields []string) error {
	if len(fields) < 1 {
		return errInvalidCommand
	}

	if len(fields) > 1 {
		return dm.Put(fields[0], strings.Join(fields[1:], " "))
	}
	return dm.Put(fields[0], nil)
}

func (c *CLI) evalPutex(dm *client.DMap, fields []string) error {
	if len(fields) < 3 {
		return errInvalidCommand
	}

	ttlRaw := fields[len(fields)-1]
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return err
	}

	key := fields[0]
	value := strings.Join(fields[1:len(fields)-1], " ")
	return dm.PutEx(key, value, ttl)
}

func (c *CLI) evalGet(dm *client.DMap, fields []string) error {
	key := strings.Join(fields, " ")
	value, err := dm.Get(key)
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%v\n", value))
	return nil
}

func (c *CLI) evalDelete(dm *client.DMap, fields []string) error {
	key := strings.Join(fields, " ")
	return dm.Delete(key)
}

func (c *CLI) evalIncr(dm *client.DMap, fields []string) error {
	if len(fields) < 1 {
		return errInvalidCommand
	}
	if len(fields) < 2 {
		return fmt.Errorf("%w: missing delta", errInvalidCommand)
	}

	key, raw := fields[0], strings.Join(fields[1:], " ")
	delta, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("invalid delta: %w", err)
	}

	var current int
	current, err = dm.Incr(key, delta)
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%d\n", current))
	return nil
}

func (c *CLI) evalDestroy(dm *client.DMap) error {
	return dm.Destroy()
}

func (c *CLI) evalDecr(dm *client.DMap, fields []string) error {
	if len(fields) < 1 {
		return errInvalidCommand
	}
	if len(fields) < 2 {
		return fmt.Errorf("%w: missing delta", errInvalidCommand)
	}

	key, raw := fields[0], strings.Join(fields[1:], " ")
	delta, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("invalid delta: %w", err)
	}

	var current int
	current, err = dm.Decr(key, delta)
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%d\n", current))
	return nil
}

func (c *CLI) evalExpire(dm *client.DMap, fields []string) error {
	if len(fields) <= 1 {
		return errInvalidCommand
	}

	key, ttlRaw := fields[0], fields[1]
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return err
	}
	return dm.Expire(key, ttl)
}

func (c *CLI) evalGetPut(dm *client.DMap, fields []string) error {
	if len(fields) <= 1 {
		return errInvalidCommand
	}

	key, value := fields[0], strings.Join(fields[1:], " ")
	current, err := dm.GetPut(key, value)
	if err != nil {
		return err
	}
	c.print(fmt.Sprintf("%v\n", current))
	return nil
}

func strToCond(raw string) (cond int16, err error) {
	if raw == "ifNotFound" {
		return olric.IfNotFound, nil
	} else if raw == "ifFound" {
		return olric.IfFound, nil
	}
	return 0, fmt.Errorf("invalid condition: %s", raw)
}

func (c *CLI) evalPutIf(dm *client.DMap, fields []string) error {
	if len(fields) < 3 {
		return errInvalidCommand
	}

	rawcond := fields[len(fields)-1]
	cond, err := strToCond(rawcond)
	if err != nil {
		return err
	}
	key := fields[0]
	value := strings.Join(fields[1:len(fields)-1], " ")
	return dm.PutIf(key, value, cond)
}

func (c *CLI) evalPutIfEx(dm *client.DMap, fields []string) error {
	if len(fields) < 4 {
		return errInvalidCommand
	}

	rawcond := fields[len(fields)-1]
	cond, err := strToCond(rawcond)
	if err != nil {
		return err
	}

	key := fields[0]
	value := strings.Join(fields[1:len(fields)-2], " ")
	rawttl := fields[len(fields)-2]

	ttl, err := time.ParseDuration(rawttl)
	if err != nil {
		return err
	}
	return dm.PutIfEx(key, value, ttl, cond)
}

func (c *CLI) evaluate(dmap, line string) error {
	line = strings.TrimSpace(line)
	fields := strings.Fields(line)
	cmd, fields := fields[0], fields[1:]
	if len(fields) < 1 {
		return errInvalidCommand
	}

	dm := c.client.NewDMap(dmap)
	switch {
	case cmd == cmdPut:
		return c.evalPut(dm, fields)
	case cmd == cmdPutEx:
		return c.evalPutex(dm, fields)
	case cmd == cmdGet:
		return c.evalGet(dm, fields)
	case cmd == cmdDelete:
		return c.evalDelete(dm, fields)
	case cmd == cmdDestroy:
		return c.evalDestroy(dm)
	case cmd == cmdIncr:
		return c.evalIncr(dm, fields)
	case cmd == cmdDecr:
		return c.evalDecr(dm, fields)
	case cmd == cmdExpire:
		return c.evalExpire(dm, fields)
	case cmd == cmdGetPut:
		return c.evalGetPut(dm, fields)
	case cmd == cmdPutIf:
		return c.evalPutIf(dm, fields)
	case cmd == cmdPutIfEx:
		return c.evalPutIfEx(dm, fields)
	case cmd == cmdGetEntry:
		return c.evalGetEntry(dm, fields)
	default:
		return fmt.Errorf("invalid command")
	}
}
