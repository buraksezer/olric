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

/*Package olric provides distributed cache and in-memory key/value data store.
It can be used both as an embedded Go library and as a language-independent
service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across
a cluster of computers.

Olric is designed to be a distributed cache. But it also provides distributed
topics, data replication, failure detection and simple anti-entropy services.
So it can be used as an ordinary key/value data store to scale your cloud
application.*/
package olric

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/internal/checkpoint"
	"github.com/buraksezer/olric/internal/cluster/balancer"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/dtopic"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/streams"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.4.0-beta.3"

var (
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrInternalServerError means that something unintentionally went
	// wrong while processing the request.
	ErrInternalServerError = errors.New("internal server error")

	// ErrUnknownOperation means that an unidentified message has been
	// received from a client.
	ErrUnknownOperation = neterrors.New(protocol.StatusErrUnknownOperation, "unknown operation")

	ErrServerGone = errors.New("server is gone")

	ErrInvalidArgument = errors.New("invalid argument")

	ErrKeyTooLarge = errors.New("key too large")

	ErrNotImplemented = errors.New("not implemented")
)

type services struct {
	dtopic *dtopic.Service
	dmap   *dmap.Service
}

// Olric implements a distributed, in-memory and embeddable key/value store.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name       string
	env        *environment.Environment
	config     *config.Config
	log        *flog.Logger
	hasher     hasher.Hasher
	serializer serializer.Serializer

	// Logical units for data storage
	primary *partitions.Partitions
	backup  *partitions.Partitions

	// Matches opcodes to functions. It's somewhat like an HTTP request
	// multiplexer
	operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)

	// Internal TCP server and its client for peer-to-peer communication.
	client *transport.Client
	server *transport.Server

	rt       *routingtable.RoutingTable
	balancer *balancer.Balancer

	services *services

	// Bidirectional stream sockets for Olric clients and nodes.
	streams *streams.Streams

	// Structures for flow control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	started func()
}

// New creates a new Olric instance, otherwise returns an error.
func New(c *config.Config) (*Olric, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	err := c.Sanitize()
	if err != nil {
		return nil, err
	}
	err = c.Validate()
	if err != nil {
		return nil, err
	}
	err = c.SetupNetworkConfig()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr,
		strconv.Itoa(c.BindPort))

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.LogLevel)),
		Writer:   c.LogOutput,
	}
	c.Logger.SetOutput(filter)

	e := environment.New()
	e.Set("config", c)

	// Set the hash function. Olric distributes keys over partitions by hashing.
	partitions.SetHashFunc(c.Hasher)

	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	e.Set("logger", flogger)

	// Start a concurrent TCP server
	sc := &transport.ServerConfig{
		BindAddr:        c.BindAddr,
		BindPort:        c.BindPort,
		KeepAlivePeriod: c.KeepAlivePeriod,
		GracefulPeriod:  10 * time.Second,
	}
	client := transport.NewClient(c.Client)
	e.Set("client", client)

	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("locker", locker.New())
	e.Set("streams", streams.New(e))
	srv := transport.NewServer(sc, flogger)
	ctx, cancel := context.WithCancel(context.Background())
	db := &Olric{
		name:       c.MemberlistConfig.Name,
		env:        e,
		ctx:        ctx,
		cancel:     cancel,
		log:        flogger,
		config:     c,
		hasher:     c.Hasher,
		serializer: c.Serializer,
		client:     e.Get("client").(*transport.Client),
		primary:    e.Get("primary").(*partitions.Partitions),
		backup:     e.Get("backup").(*partitions.Partitions),
		streams:    e.Get("streams").(*streams.Streams),
		operations: make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder)),
		server:     srv,
		started:    c.Started,
	}

	db.rt = routingtable.New(e)
	e.Set("routingtable", db.rt)

	db.balancer = balancer.New(e)

	// Add Services
	dt, err := dtopic.NewService(e)
	if err != nil {
		return nil, err
	}

	dm, err := dmap.NewService(e)
	if err != nil {
		return nil, err
	}

	db.services = &services{
		dtopic: dt.(*dtopic.Service),
		dmap:   dm.(*dmap.Service),
	}

	// Add callback functions to routing table.
	db.rt.AddCallback(db.balancer.Balance)
	db.server.SetDispatcher(db.requestDispatcher)
	db.registerOperations()
	return db, nil
}

func (db *Olric) requestDispatcher(w, r protocol.EncodeDecoder) {
	// Check bootstrapping status
	// Exclude protocol.OpUpdateRouting. The node is bootstrapped by this
	// operation.
	if r.OpCode() != protocol.OpUpdateRouting {
		if err := db.isOperable(); err != nil {
			neterrors.ErrorResponse(w, err)
			return
		}
	}

	// Run the incoming command.
	f, ok := db.operations[r.OpCode()]
	if !ok {
		neterrors.ErrorResponse(w, ErrUnknownOperation)
		return
	}
	f(w, r)
}

// callStartedCallback checks passed checkpoint count and calls the callback
// function.
func (db *Olric) callStartedCallback() {
	defer db.wg.Done()

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			if checkpoint.AllPassed() {
				if db.started != nil {
					db.started()
				}
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

// isOperable controls bootstrapping status and cluster quorum to prevent split-brain syndrome.
func (db *Olric) isOperable() error {
	if err := db.rt.CheckMemberCountQuorum(); err != nil {
		return publicClusterError(err)
	}
	// An Olric node has to be bootstrapped to function properly.
	return db.rt.CheckBootstrap()
}

// Start starts background servers and joins the cluster. You still need to call
// Shutdown method if Start function returns an early error.
func (db *Olric) Start() error {
	errGr, ctx := errgroup.WithContext(context.Background())

	// Start the TCP server
	errGr.Go(func() error {
		return db.server.ListenAndServe()
	})

	select {
	case <-db.server.StartedCtx.Done():
		// TCP server is started
		checkpoint.Pass()
	case <-ctx.Done():
		if err := db.Shutdown(context.Background()); err != nil {
			db.log.V(2).Printf("[ERROR] Failed to Shutdown: %v", err)
		}
		return errGr.Wait()
	}

	// Start routing table service and member discovery subsystem.
	if err := db.rt.Start(); err != nil {
		return err
	}

	// Start distributed topic service
	if err := db.services.dtopic.Start(); err != nil {
		return err
	}

	// Start distributed map service
	if err := db.services.dmap.Start(); err != nil {
		return err
	}

	// Warn the user about its choice of configuration
	if db.config.ReplicationMode == config.AsyncReplicationMode && db.config.WriteQuorum > 1 {
		db.log.V(2).
			Printf("[WARN] Olric is running in async replication mode. WriteQuorum (%d) is ineffective",
				db.config.WriteQuorum)
	}

	if db.started != nil {
		db.wg.Add(1)
		go db.callStartedCallback()
	}

	db.log.V(2).Printf("[INFO] Node name in the cluster: %s", db.name)
	if db.config.Interface != "" {
		db.log.V(2).Printf("[INFO] Olric uses interface: %s", db.config.Interface)
	}
	db.log.V(2).Printf("[INFO] Olric bindAddr: %s, bindPort: %d", db.config.BindAddr, db.config.BindPort)
	return errGr.Wait()
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	db.cancel()

	var result error

	if err := db.services.dtopic.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	if err := db.services.dmap.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	db.log.V(2).Printf("[INFO] Closing active streams")
	if err := db.streams.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	db.balancer.Shutdown()

	if err := db.server.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	if err := db.rt.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	db.wg.Wait()

	// db.name will be shown as empty string, if the program is killed before
	// bootstrapping.
	db.log.V(2).Printf("[INFO] %s is gone", db.name)
	return result
}

func publicClusterError(err error) error {
	switch err {
	case routingtable.ErrClusterQuorum:
		return ErrClusterQuorum
	case routingtable.ErrServerGone:
		return ErrServerGone
	case routingtable.ErrOperationTimeout:
		return ErrOperationTimeout
	default:
		return err
	}
}
