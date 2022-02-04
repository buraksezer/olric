// The MIT License (MIT)
//
// Copyright (c) 2016 Josh Baker
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package server

import (
	"fmt"
	"strings"

	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

// ServeMux is an RESP command multiplexer.
type ServeMux struct {
	handlers map[string]redcon.Handler
}

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]redcon.Handler),
	}
}

// HandleFunc registers the handler function for the given command.
func (m *ServeMux) HandleFunc(command string, handler redcon.Handler) {
	if handler == nil {
		panic("olric: nil handler")
	}
	m.Handle(command, handler)
}

// Handle registers the handler for the given command.
// If a handler already exists for command, Handle panics.
func (m *ServeMux) Handle(command string, handler redcon.Handler) {
	if command == "" {
		panic("olric: invalid command")
	}
	if handler == nil {
		panic("olric: nil handler")
	}
	if _, exist := m.handlers[command]; exist {
		panic("olric: multiple registrations for " + command)
	}

	m.handlers[command] = handler
}

// ServeRESP dispatches the command to the handler.
func (m *ServeMux) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(util.BytesToString(cmd.Args[0]))

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
		return
	}

	if command == "pubsub" {
		if len(cmd.Args) < 2 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
			return
		}
		command = fmt.Sprintf("%s %s", command, util.BytesToString(cmd.Args[1]))
	}

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
		return
	}

	conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
}
