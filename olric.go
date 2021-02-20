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

/*Package olric provides distributed cache and in-memory key/value data store. It can be used both as an embedded Go
library and as a language-independent service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across a cluster of computers.

Olric is designed to be a distributed cache. But it also provides distributed topics, data replication, failure detection
and simple anti-entropy services. So it can be used as an ordinary key/value data store to scale your cloud application.*/
package olric

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotFound is returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrInternalServerError means that something unintentionally went wrong while processing the request.
	ErrInternalServerError = errors.New("internal server error")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

	// ErrUnknownOperation means that an unidentified message has been received from a client.
	ErrUnknownOperation = errors.New("unknown operation")

	ErrServerGone = errors.New("server is gone")

	ErrInvalidArgument = errors.New("invalid argument")

	ErrKeyTooLarge = errors.New("key too large")

	ErrNotImplemented = errors.New("not implemented")
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.3.0"

const (
	nilTimeout                = 0 * time.Second
	requiredCheckpoints int32 = 2
)

// A full list of alive members. It's required for Pub/Sub and event dispatching systems.
type members struct {
	mtx sync.RWMutex
	m   map[uint64]discovery.Member
}

// Olric implements a distributed, in-memory and embeddable key/value store and cache.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name string

	// Currently owned partition count. Approximate LRU implementation
	// uses that.
	ownedPartitionCount uint64

	// These values is useful to control operation status.
	bootstrapped int32

	// numMembers is used to check cluster quorum.
	numMembers int32

	// Number of successfully passed checkpoints
	passedCheckpoints int32

	// this defines this Olric node in the cluster.
	this   discovery.Member
	config *config.Config
	log    *flog.Logger

	// hasher may be defined by the user. The default one is xxhash
	hasher hasher.Hasher

	// Fine-grained lock implementation. Useful to implement atomic operations
	// and distributed, optimistic lock implementation.
	locker     *locker.Locker
	serializer serializer.Serializer
	discovery  *discovery.Discovery

	// consistent hash ring implementation.
	consistent *consistent.Consistent

	// Logical units for data storage
	partitions map[uint64]*partition
	backups    map[uint64]*partition

	// Matches opcodes to functions. It's somewhat like an HTTP request multiplexer
	operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)

	// Internal TCP server and its client for peer-to-peer communication.
	client *transport.Client
	server *transport.Server

	// A full list of alive members. It's required for Pub/Sub and event dispatching systems.
	members members

	// Dispatch topic messages
	dtopic *dtopic

	// Bidirectional stream sockets for Olric clients and nodes.
	streams *streams

	// Structures for flow control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	started func()
}

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

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
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))

	cfg := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}
	client := transport.NewClient(c.Client)

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.LogLevel)),
		Writer:   c.LogOutput,
	}
	c.Logger.SetOutput(filter)

	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	// Start a concurrent TCP server
	sc := &transport.ServerConfig{
		BindAddr:        c.BindAddr,
		BindPort:        c.BindPort,
		KeepAlivePeriod: c.KeepAlivePeriod,
		GracefulPeriod:  10 * time.Second,
	}
	srv := transport.NewServer(sc, flogger)
	ctx, cancel := context.WithCancel(context.Background())
	db := &Olric{
		name:       c.MemberlistConfig.Name,
		ctx:        ctx,
		cancel:     cancel,
		log:        flogger,
		config:     c,
		hasher:     c.Hasher,
		locker:     locker.New(),
		serializer: c.Serializer,
		consistent: consistent.New(nil, cfg),
		client:     client,
		partitions: make(map[uint64]*partition),
		backups:    make(map[uint64]*partition),
		operations: make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder)),
		server:     srv,
		members:    members{m: make(map[uint64]discovery.Member)},
		dtopic:     newDTopic(ctx),
		streams:    &streams{m: make(map[uint64]*stream)},
		started:    c.Started,
	}

	db.server.SetDispatcher(db.requestDispatcher)

	// Create all the partitions. It's read-only. No need for locking.
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.partitions[i] = &partition{id: i}
	}

	// Create all the backup partitions. It's read-only. No need for locking.
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.backups[i] = &partition{
			id:     i,
			backup: true,
		}
	}

	db.registerOperations()
	return db, nil
}

func (db *Olric) passCheckpoint() {
	atomic.AddInt32(&db.passedCheckpoints, 1)
}

func (db *Olric) requestDispatcher(w, r protocol.EncodeDecoder) {
	// Check bootstrapping status
	// Exclude protocol.OpUpdateRouting. The node is bootstrapped by this operation.
	if r.OpCode() != protocol.OpUpdateRouting {
		if err := db.checkOperationStatus(); err != nil {
			db.errorResponse(w, err)
			return
		}
	}

	// Run the incoming command.
	f, ok := db.operations[r.OpCode()]
	if !ok {
		db.errorResponse(w, ErrUnknownOperation)
		return
	}
	f(w, r)
}

// bootstrapCoordinator prepares the very first routing table and bootstraps the coordinator node.
func (db *Olric) bootstrapCoordinator() error {
	routingMtx.Lock()
	defer routingMtx.Unlock()

	table := db.distributePartitions()
	_, err := db.updateRoutingTableOnCluster(table)
	if err == nil {
		// The coordinator bootstraps itself.
		atomic.StoreInt32(&db.bootstrapped, 1)
		db.log.V(2).Printf("[INFO] The cluster coordinator has been bootstrapped")
	}
	return err
}

// startDiscovery initializes and starts discovery subsystem.
func (db *Olric) startDiscovery() error {
	d, err := discovery.New(db.log, db.config)
	if err != nil {
		return err
	}
	err = d.Start()
	if err != nil {
		return err
	}
	db.discovery = d

	attempts := 0
	for attempts < db.config.MaxJoinAttempts {
		if !db.isAlive() {
			return nil
		}

		attempts++
		n, err := db.discovery.Join()
		if err == nil {
			db.log.V(2).Printf("[INFO] Join completed. Synced with %d initial nodes", n)
			break
		}

		db.log.V(2).Printf("[ERROR] Join attempt returned error: %s", err)
		if atomic.LoadInt32(&db.bootstrapped) == 1 {
			db.log.V(2).Printf("[INFO] Bootstrapped by the cluster coordinator")
			break
		}

		db.log.V(2).Printf("[INFO] Awaits for %s to join again (%d/%d)",
			db.config.JoinRetryInterval, attempts, db.config.MaxJoinAttempts)
		<-time.After(db.config.JoinRetryInterval)
	}

	this, err := db.discovery.FindMemberByName(db.name)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to get this node in cluster: %v", err)
		serr := db.discovery.Shutdown()
		if serr != nil {
			return serr
		}
		return err
	}
	db.this = this

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	db.storeNumMembers()

	db.wg.Add(1)
	go db.listenMemberlistEvents(d.ClusterEvents)

	// Check member count quorum now. If there is no enough peers to work, wait forever.
	for {
		err := db.checkMemberCountQuorum()
		if err == nil {
			// It's OK. Continue as usual.
			break
		}

		db.log.V(2).Printf("[ERROR] Inoperable node: %v", err)
		select {
		// TODO: Consider making this parametric
		case <-time.After(time.Second):
		case <-db.ctx.Done():
			// the server is gone
			return nil
		}
	}

	db.members.mtx.Lock()
	db.members.m[db.this.ID] = db.this
	db.members.mtx.Unlock()

	db.consistent.Add(db.this)
	if db.discovery.IsCoordinator() {
		err = db.bootstrapCoordinator()
		if err == consistent.ErrInsufficientMemberCount {
			db.log.V(2).Printf("[ERROR] Failed to bootstrap the coordinator node: %v", err)
			// Olric will try to form a cluster again.
			err = nil
		}
		if err != nil {
			return err
		}
	}

	if db.config.Interface != "" {
		db.log.V(2).Printf("[INFO] Olric uses interface: %s", db.config.Interface)
	}

	db.log.V(2).Printf("[INFO] Olric bindAddr: %s, bindPort: %d",
		db.config.BindAddr,
		db.config.BindPort)

	if db.config.MemberlistInterface != "" {
		db.log.V(2).Printf("[INFO] Memberlist uses interface: %s", db.config.MemberlistInterface)
	}

	db.log.V(2).Printf("[INFO] Memberlist bindAddr: %s, bindPort: %d",
		db.config.MemberlistConfig.BindAddr,
		db.config.MemberlistConfig.BindPort)

	db.log.V(2).Printf("[INFO] Cluster coordinator: %s", db.discovery.GetCoordinator())
	return nil
}

// callStartedCallback checks passed checkpoint count and calls the callback function.
func (db *Olric) callStartedCallback() {
	defer db.wg.Done()

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(10 *  time.Millisecond)
		select {
		case <-timer.C:
			if requiredCheckpoints == atomic.LoadInt32(&db.passedCheckpoints) {
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

func (db *Olric) errorResponse(w protocol.EncodeDecoder, err error) {
	getError := func(err interface{}) []byte {
		switch val := err.(type) {
		case string:
			return []byte(val)
		case error:
			return []byte(val.Error())
		default:
			return nil
		}
	}
	w.SetValue(getError(err))

	switch {
	case err == ErrWriteQuorum, errors.Is(err, ErrWriteQuorum):
		w.SetStatus(protocol.StatusErrWriteQuorum)
	case err == ErrReadQuorum, errors.Is(err, ErrReadQuorum):
		w.SetStatus(protocol.StatusErrReadQuorum)
	case err == ErrNoSuchLock, errors.Is(err, ErrNoSuchLock):
		w.SetStatus(protocol.StatusErrNoSuchLock)
	case err == ErrLockNotAcquired, errors.Is(err, ErrLockNotAcquired):
		w.SetStatus(protocol.StatusErrLockNotAcquired)
	case err == ErrKeyNotFound, err == storage.ErrKeyNotFound:
		w.SetStatus(protocol.StatusErrKeyNotFound)
	case errors.Is(err, ErrKeyNotFound), errors.Is(err, storage.ErrKeyNotFound):
		w.SetStatus(protocol.StatusErrKeyNotFound)
	case err == ErrKeyTooLarge, err == storage.ErrKeyTooLarge:
		w.SetStatus(protocol.StatusErrKeyTooLarge)
	case errors.Is(err, ErrKeyTooLarge), errors.Is(err, storage.ErrKeyTooLarge):
		w.SetStatus(protocol.StatusErrKeyTooLarge)
	case err == ErrOperationTimeout, errors.Is(err, ErrOperationTimeout):
		w.SetStatus(protocol.StatusErrOperationTimeout)
	case err == ErrKeyFound, errors.Is(err, ErrKeyFound):
		w.SetStatus(protocol.StatusErrKeyFound)
	case err == ErrClusterQuorum, errors.Is(err, ErrClusterQuorum):
		w.SetStatus(protocol.StatusErrClusterQuorum)
	case err == ErrUnknownOperation, errors.Is(err, ErrUnknownOperation):
		w.SetStatus(protocol.StatusErrUnknownOperation)
	case err == ErrEndOfQuery, errors.Is(err, ErrEndOfQuery):
		w.SetStatus(protocol.StatusErrEndOfQuery)
	case err == ErrServerGone, errors.Is(err, ErrServerGone):
		w.SetStatus(protocol.StatusErrServerGone)
	case err == ErrInvalidArgument, errors.Is(err, ErrInvalidArgument):
		w.SetStatus(protocol.StatusErrInvalidArgument)
	case err == ErrNotImplemented, errors.Is(err, ErrNotImplemented):
		w.SetStatus(protocol.StatusErrNotImplemented)
	default:
		w.SetStatus(protocol.StatusInternalServerError)
	}
}

func (db *Olric) requestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	resp, err := db.client.RequestTo(addr, req)
	if err != nil {
		return nil, err
	}

	status := resp.Status()

	switch {
	case status == protocol.StatusOK:
		return resp, nil
	case status == protocol.StatusInternalServerError:
		return nil, errors.Wrap(ErrInternalServerError, string(resp.Value()))
	case status == protocol.StatusErrNoSuchLock:
		return nil, ErrNoSuchLock
	case status == protocol.StatusErrLockNotAcquired:
		return nil, ErrLockNotAcquired
	case status == protocol.StatusErrKeyNotFound:
		return nil, ErrKeyNotFound
	case status == protocol.StatusErrWriteQuorum:
		return nil, ErrWriteQuorum
	case status == protocol.StatusErrReadQuorum:
		return nil, ErrReadQuorum
	case status == protocol.StatusErrOperationTimeout:
		return nil, ErrOperationTimeout
	case status == protocol.StatusErrKeyFound:
		return nil, ErrKeyFound
	case status == protocol.StatusErrClusterQuorum:
		return nil, ErrClusterQuorum
	case status == protocol.StatusErrEndOfQuery:
		return nil, ErrEndOfQuery
	case status == protocol.StatusErrUnknownOperation:
		return nil, ErrUnknownOperation
	case status == protocol.StatusErrServerGone:
		return nil, ErrServerGone
	case status == protocol.StatusErrInvalidArgument:
		return nil, ErrInvalidArgument
	case status == protocol.StatusErrKeyTooLarge:
		return nil, ErrKeyTooLarge
	case status == protocol.StatusErrNotImplemented:
		return nil, ErrNotImplemented
	}
	return nil, fmt.Errorf("unknown status code: %d", status)
}

func (db *Olric) isAlive() bool {
	select {
	case <-db.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

// checkBootstrap is called for every request and checks whether the node is bootstrapped.
// It has to be very fast for a smooth operation.
func (db *Olric) checkBootstrap() error {
	// check it immediately
	if atomic.LoadInt32(&db.bootstrapped) == 1 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.config.BootstrapTimeout)
	defer cancel()

	// This loop only works for the first moments of the process.
	for {
		if atomic.LoadInt32(&db.bootstrapped) == 1 {
			return nil
		}
		<-time.After(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ErrOperationTimeout
		default:
		}
	}
}

// storeNumMembers assigns the current number of members in the cluster to a variable.
func (db *Olric) storeNumMembers() {
	// Calling NumMembers in every request is quite expensive.
	// It's rarely updated. Just call this when the membership info changed.
	nr := int32(db.discovery.NumMembers())
	atomic.StoreInt32(&db.numMembers, nr)
}

func (db *Olric) checkMemberCountQuorum() error {
	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	nr := atomic.LoadInt32(&db.numMembers)
	if db.config.MemberCountQuorum > nr {
		return ErrClusterQuorum
	}
	return nil
}

// checkOperationStatus controls bootstrapping status and cluster quorum to prevent split-brain syndrome.
func (db *Olric) checkOperationStatus() error {
	if err := db.checkMemberCountQuorum(); err != nil {
		return err
	}
	// An Olric node has to be bootstrapped to function properly.
	return db.checkBootstrap()
}

// Start starts background servers and joins the cluster. You still need to call Shutdown method if
// Start function returns an early error.
func (db *Olric) Start() error {
	errCh := make(chan error, 1)
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		errCh <- db.server.ListenAndServe()
	}()

	<-db.server.StartCh
	select {
	case err := <-errCh:
		return err
	default:
	}
	// TCP server is started
	db.passCheckpoint()

	if err := db.startDiscovery(); err != nil {
		return err
	}
	// Memberlist is started and this node joined the cluster.
	db.passCheckpoint()

	// Warn the user about its choice of configuration
	if db.config.ReplicationMode == config.AsyncReplicationMode && db.config.WriteQuorum > 1 {
		db.log.V(2).
			Printf("[WARN] Olric is running in async replication mode. WriteQuorum (%d) is ineffective",
				db.config.WriteQuorum)
	}

	db.log.V(2).Printf("[INFO] Node name in the cluster: %s", db.name)

	// Start periodic tasks.
	db.wg.Add(2)
	go db.updateRoutingPeriodically()
	go db.evictKeysAtBackground()

	if db.started != nil {
		db.wg.Add(1)
		go db.callStartedCallback()
	}

	return <-errCh
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	db.cancel()

	var result error

	db.streams.mu.RLock()
	db.log.V(2).Printf("[INFO] Closing active streams")
	for _, s := range db.streams.m {
		s.close()
	}
	db.streams.mu.RUnlock()

	if err := db.server.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	if db.discovery != nil {
		err := db.discovery.Shutdown()
		if err != nil {
			result = multierror.Append(result, err)
		}
	}

	db.wg.Wait()

	// If the user kills the server before bootstrapping, db.this is going to empty.
	db.log.V(2).Printf("[INFO] %s is gone", db.name)
	return result
}

func getTTL(timeout time.Duration) int64 {
	// convert nanoseconds to milliseconds
	return (timeout.Nanoseconds() + time.Now().UnixNano()) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	// convert nanoseconds to milliseconds
	return (time.Now().UnixNano() / 1000000) >= ttl
}

// cmpMembersByID returns true if two members denote the same member in the cluster.
func cmpMembersByID(one, two discovery.Member) bool {
	// ID variable is calculated by combining member's name and birthdate
	return one.ID == two.ID
}

// cmpMembersByName returns true if the two members has the same name in the cluster.
// This function is intended to redirect the requests to the partition owner.
func cmpMembersByName(one, two discovery.Member) bool {
	return one.NameHash == two.NameHash
}
