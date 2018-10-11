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

package cli

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/buraksezer/olricdb/client"
	"github.com/chzyer/readline"
	"golang.org/x/net/http2"
)

type CLI struct {
	uri    string
	client *client.Client
	output io.Writer
}

func New(uri string, insecureSkipVerify bool, timeouts string) (*CLI, error) {
	timeout, err := time.ParseDuration(timeouts)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	var hc *http.Client
	if parsed.Scheme == "https" {
		tc := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
		dialTLS := func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			d := &net.Dialer{Timeout: timeout}
			return tls.DialWithDialer(d, network, addr, cfg)
		}
		hc = &http.Client{
			Transport: &http2.Transport{
				DialTLS:         dialTLS,
				TLSClientConfig: tc,
			},
			Timeout: timeout,
		}
	} else {
		hc = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					KeepAlive: 5 * time.Minute,
					Timeout:   timeout,
				}).DialContext,
			},
		}
	}

	c, err := client.New([]string{uri}, hc, nil)
	if err != nil {
		return nil, err
	}
	return &CLI{
		uri:    uri,
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
)

func extractKey(str string) (string, error) {
	res := regexp.MustCompile(`"([^"]*)"`).FindStringSubmatch(str)
	if len(res) < 2 {
		return "", errors.New("invalid command")
	}
	return res[1], nil
}

func (c *CLI) print(msg string) {
	io.WriteString(c.output, msg)
}

func (c *CLI) Start() error {
	var historyFile string
	home := os.Getenv("HOME")
	if home != "" {
		historyFile = path.Join(home, ".olricdbcli_history")
	} else {
		c.print("[WARN] $HOME is empty.\n")
	}
	prompt := fmt.Sprintf("[%s] \033[31m»\033[0m ", c.uri)
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

		if len(dmap) == 0 {
			c.print("[ERROR] Call 'use' command before accessing database.\n")
			continue
		}

		switch {
		case strings.HasPrefix(line, "put "):
			tmp := strings.TrimLeft(line, "put ")
			tmp = strings.TrimSpace(tmp)
			var key, value string
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
				value = strings.TrimSpace(rval[0])
			} else {
				res := strings.SplitN(tmp, " ", 2)
				if len(res) < 2 {
					c.print(fmt.Sprintf("[ERROR] invalid command: %s\n", line))
					continue
				}
				key, value = res[0], res[1]
			}
			if err := c.client.Put(dmap, key, value); err != nil {
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

			if err := c.client.PutEx(dmap, key, value, ttl); err != nil {
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
			value, err := c.client.Get(dmap, key)
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
			err := c.client.Delete(dmap, key)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Delete on %s with key %s: %v\n", dmap, key, err))
				continue
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "destroy"):
			err := c.client.Destroy(dmap)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Destroy on %s: %v\n", dmap, err))
				continue
			}
			c.print("OK\n")
		default:
			c.print("[ERROR] Invalid command. Available commands: put, putex, get, delete, destroy and exit.\n")
		}
	}
	return nil
}
