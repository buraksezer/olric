// Copyright 2018-2022 Burak Sezer
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

package server

import (
	"fmt"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

type ServeMuxWrapper struct {
	mux     *ServeMux
	precond func(conn redcon.Conn, cmd redcon.Command) bool
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as RESP handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(conn redcon.Conn, cmd redcon.Command)

type Handler struct {
	handler func(conn redcon.Conn, cmd redcon.Command)
	precond func(conn redcon.Conn, cmd redcon.Command) bool
}

// ServeRESP calls f(w, r)
func (h Handler) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	CommandsTotal.Increase(1)

	if len(cmd.Args) == 0 {
		// A client may form a bad message, prevent panicking.
		h.handler(conn, cmd)
		return
	}
	command := util.BytesToString(cmd.Args[0])
	if command == "pubsub" || command == "PUBSUB" {
		command = fmt.Sprintf("%s %s", command, util.BytesToString(cmd.Args[1]))
	}
	// The node is updated by UpdateRoutingCmd. So it's a precondition for
	// an operable node.
	if command == protocol.Internal.UpdateRouting {
		h.handler(conn, cmd)
		return
	}

	if h.precond == nil {
		// No precondition
		h.handler(conn, cmd)
		return
	}

	if h.precond(conn, cmd) {
		h.handler(conn, cmd)
	}
}

// HandleFunc registers the handler function for the given command.
func (m *ServeMuxWrapper) HandleFunc(command string, handler func(conn redcon.Conn, cmd redcon.Command)) {
	if handler == nil {
		panic("server: nil handler")
	}
	m.mux.Handle(command, Handler{
		handler: handler,
		precond: m.precond,
	})
}
