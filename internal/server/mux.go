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
		command = fmt.Sprintf("%s %s", command, util.BytesToString(cmd.Args[1]))
	}

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
		return
	}

	conn.WriteError("ERR unknown command '" + command + "'")
}
