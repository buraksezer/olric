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

package pubsub

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/btree"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

// PubSub is a Redis compatible pub/sub server
type PubSub struct {
	mu     sync.RWMutex
	nextid uint64
	initd  bool
	chans  *btree.BTree
	conns  map[redcon.Conn]*pubSubConn

	// callbacks
	unsubscribeCallback  func()
	punsubscribeCallback func()
}

// Subscribe a connection to PubSub
func (ps *PubSub) Subscribe(conn redcon.Conn, channel string) {
	ps.subscribe(conn, false, channel)
}

// Psubscribe a connection to PubSub
func (ps *PubSub) Psubscribe(conn redcon.Conn, channel string) {
	ps.subscribe(conn, true, channel)
}

// Publish a message to subscribers
func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if !ps.initd {
		return 0
	}
	var sent int
	// write messages to all clients that are subscribed on the channel
	pivot := &pubSubEntry{pattern: false, channel: channel}
	ps.chans.Ascend(pivot, func(item interface{}) bool {
		entry := item.(*pubSubEntry)
		if entry.channel != pivot.channel || entry.pattern != pivot.pattern {
			return false
		}
		entry.sconn.writeMessage(entry.pattern, "", channel, message)
		sent++
		return true
	})

	// match on and write all psubscribe clients
	pivot = &pubSubEntry{pattern: true}
	ps.chans.Ascend(pivot, func(item interface{}) bool {
		entry := item.(*pubSubEntry)
		if match.Match(channel, entry.channel) {
			entry.sconn.writeMessage(entry.pattern, entry.channel, channel,
				message)
		}
		sent++
		return true
	})

	return sent
}

type pubSubConn struct {
	id      uint64
	mu      sync.Mutex
	conn    redcon.Conn
	dconn   redcon.DetachedConn
	entries map[*pubSubEntry]bool
}

type pubSubEntry struct {
	pattern bool
	sconn   *pubSubConn
	channel string
}

func (sconn *pubSubConn) writeMessage(pat bool, pchan, channel, msg string) {
	sconn.mu.Lock()
	defer sconn.mu.Unlock()
	if pat {
		sconn.dconn.WriteArray(4)
		sconn.dconn.WriteBulkString("pmessage")
		sconn.dconn.WriteBulkString(pchan)
		sconn.dconn.WriteBulkString(channel)
		sconn.dconn.WriteBulkString(msg)
	} else {
		sconn.dconn.WriteArray(3)
		sconn.dconn.WriteBulkString("message")
		sconn.dconn.WriteBulkString(channel)
		sconn.dconn.WriteBulkString(msg)
	}
	sconn.dconn.Flush()
}

// bgrunner runs in the background and reads incoming commands from the
// detached client.
func (sconn *pubSubConn) bgrunner(ps *PubSub) {
	defer func() {
		// client connection has ended, disconnect from the PubSub instances
		// and close the network connection.
		ps.mu.Lock()
		defer ps.mu.Unlock()
		for entry := range sconn.entries {
			ps.chans.Delete(entry)
		}
		delete(ps.conns, sconn.conn)
		sconn.mu.Lock()
		defer sconn.mu.Unlock()
		sconn.dconn.Close()
	}()
	for {
		cmd, err := sconn.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		switch strings.ToLower(string(cmd.Args[0])) {
		case "psubscribe", "subscribe":
			if len(cmd.Args) < 2 {
				func() {
					sconn.mu.Lock()
					defer sconn.mu.Unlock()
					sconn.dconn.WriteError(fmt.Sprintf("ERR wrong number of "+
						"arguments for '%s'", cmd.Args[0]))
					sconn.dconn.Flush()
				}()
				continue
			}
			command := strings.ToLower(string(cmd.Args[0]))
			for i := 1; i < len(cmd.Args); i++ {
				if command == "psubscribe" {
					ps.Psubscribe(sconn.conn, string(cmd.Args[i]))
				} else {
					ps.Subscribe(sconn.conn, string(cmd.Args[i]))
				}
			}
		case "unsubscribe", "punsubscribe":
			pattern := strings.ToLower(string(cmd.Args[0])) == "punsubscribe"
			if len(cmd.Args) == 1 {
				ps.unsubscribe(sconn.conn, pattern, true, "")
			} else {
				for i := 1; i < len(cmd.Args); i++ {
					channel := string(cmd.Args[i])
					ps.unsubscribe(sconn.conn, pattern, false, channel)
				}
			}
		case "quit":
			func() {
				sconn.mu.Lock()
				defer sconn.mu.Unlock()
				sconn.dconn.WriteString("OK")
				sconn.dconn.Flush()
				sconn.dconn.Close()
			}()
			return
		case "ping":
			var msg string
			switch len(cmd.Args) {
			case 1:
			case 2:
				msg = string(cmd.Args[1])
			default:
				func() {
					sconn.mu.Lock()
					defer sconn.mu.Unlock()
					sconn.dconn.WriteError(fmt.Sprintf("ERR wrong number of "+
						"arguments for '%s'", cmd.Args[0]))
					sconn.dconn.Flush()
				}()
				continue
			}
			func() {
				sconn.mu.Lock()
				defer sconn.mu.Unlock()
				sconn.dconn.WriteArray(2)
				sconn.dconn.WriteBulkString("pong")
				sconn.dconn.WriteBulkString(msg)
				sconn.dconn.Flush()
			}()
		default:
			func() {
				sconn.mu.Lock()
				defer sconn.mu.Unlock()
				sconn.dconn.WriteError(fmt.Sprintf("ERR Can't execute '%s': "+
					"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are "+
					"allowed in this context", cmd.Args[0]))
				sconn.dconn.Flush()
			}()
		}
	}
}

// byEntry is a "less" function that sorts the entries in a btree. The tree
// is sorted be (pattern, channel, conn.id). All pattern=true entries are at
// the end (right) of the tree.
func byEntry(a, b interface{}) bool {
	aa := a.(*pubSubEntry)
	bb := b.(*pubSubEntry)
	if !aa.pattern && bb.pattern {
		return true
	}
	if aa.pattern && !bb.pattern {
		return false
	}
	if aa.channel < bb.channel {
		return true
	}
	if aa.channel > bb.channel {
		return false
	}
	var aid uint64
	var bid uint64
	if aa.sconn != nil {
		aid = aa.sconn.id
	}
	if bb.sconn != nil {
		bid = bb.sconn.id
	}
	return aid < bid
}

func (ps *PubSub) subscribe(conn redcon.Conn, pattern bool, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// initialize the PubSub instance
	if !ps.initd {
		ps.conns = make(map[redcon.Conn]*pubSubConn)
		ps.chans = btree.New(byEntry)
		ps.initd = true
	}

	// fetch the pubSubConn
	sconn, ok := ps.conns[conn]
	if !ok {
		// initialize a new pubSubConn, which runs on a detached connection,
		// and attach it to the PubSub channels/conn btree
		ps.nextid++
		dconn := conn.Detach()
		sconn = &pubSubConn{
			id:      ps.nextid,
			conn:    conn,
			dconn:   dconn,
			entries: make(map[*pubSubEntry]bool),
		}
		ps.conns[conn] = sconn
	}
	sconn.mu.Lock()
	defer sconn.mu.Unlock()

	// add an entry to the pubsub btree
	entry := &pubSubEntry{
		pattern: pattern,
		channel: channel,
		sconn:   sconn,
	}
	ps.chans.Set(entry)
	sconn.entries[entry] = true

	// send a message to the client
	sconn.dconn.WriteArray(3)
	if pattern {
		sconn.dconn.WriteBulkString("psubscribe")
	} else {
		sconn.dconn.WriteBulkString("subscribe")
	}
	sconn.dconn.WriteBulkString(channel)
	var count int
	for ient := range sconn.entries {
		if ient.pattern == pattern {
			count++
		}
	}
	sconn.dconn.WriteInt(count)
	sconn.dconn.Flush()

	// start the background client operation
	if !ok {
		go sconn.bgrunner(ps)
	}
}

func (ps *PubSub) unsubscribe(conn redcon.Conn, pattern, all bool, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	// fetch the pubSubConn. This must exist
	sconn := ps.conns[conn]
	sconn.mu.Lock()
	defer sconn.mu.Unlock()

	removeEntry := func(entry *pubSubEntry) {
		if entry != nil {
			ps.chans.Delete(entry)
			delete(sconn.entries, entry)
		}
		sconn.dconn.WriteArray(3)
		if pattern {
			if ps.punsubscribeCallback != nil {
				ps.punsubscribeCallback()
			}
			sconn.dconn.WriteBulkString("punsubscribe")
		} else {
			if ps.unsubscribeCallback != nil {
				ps.unsubscribeCallback()
			}
			sconn.dconn.WriteBulkString("unsubscribe")
		}
		if entry != nil {
			sconn.dconn.WriteBulkString(entry.channel)
		} else {
			sconn.dconn.WriteNull()
		}
		var count int
		for ient := range sconn.entries {
			if ient.pattern == pattern {
				count++
			}
		}
		sconn.dconn.WriteInt(count)
	}
	if all {
		// unsubscribe from all (p)subscribe entries
		var entries []*pubSubEntry
		for ient := range sconn.entries {
			if ient.pattern == pattern {
				entries = append(entries, ient)
			}
		}
		if len(entries) == 0 {
			removeEntry(nil)
		} else {
			for _, entry := range entries {
				removeEntry(entry)
			}
		}
	} else {
		// unsubscribe single channel from (p)subscribe.
		var entry *pubSubEntry
		for ient := range sconn.entries {
			if ient.pattern == pattern && ient.channel == channel {
				entry = ient
				break
			}
		}
		removeEntry(entry)
	}
	sconn.dconn.Flush()
}

func (ps *PubSub) Channels() []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.initd {
		return nil
	}

	var channels []string
	for _, sconn := range ps.conns {
		sconn.mu.Lock()
		for ient := range sconn.entries {
			if !ient.pattern {
				channels = append(channels, ient.channel)
			}
		}
		sconn.mu.Unlock()
	}

	return channels
}

func (ps *PubSub) ChannelsWithPatterns(pattern string) []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.initd {
		return nil
	}

	var channels []string
	for _, sconn := range ps.conns {
		sconn.mu.Lock()
		for ient := range sconn.entries {
			if match.Match(ient.channel, pattern) {
				channels = append(channels, ient.channel)
			}
		}
		sconn.mu.Unlock()
	}

	return channels
}

func (ps *PubSub) Numpat() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.initd {
		return 0
	}

	set := make(map[string]struct{})
	for _, sconn := range ps.conns {
		sconn.mu.Lock()
		for ient := range sconn.entries {
			if ient.pattern {
				set[ient.channel] = struct{}{}
			}
		}
		sconn.mu.Unlock()
	}

	return len(set)
}

func (ps *PubSub) Numsub(channel string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if !ps.initd {
		return 0
	}

	var result int
	for _, sconn := range ps.conns {
		sconn.mu.Lock()
		for ient := range sconn.entries {
			if ient.channel == channel {
				result++
			}
		}
		sconn.mu.Unlock()
	}

	return result
}
