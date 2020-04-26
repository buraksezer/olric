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

/*Package olric provides distributed, in-memory and embeddable key/value store, used as a database and cache.*/
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
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/internal/transport"
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
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.2.0-rc.5"

const (
	nilTimeout                = 0 * time.Second
	requiredCheckpoints int32 = 2
)

// Olric implements a distributed, in-memory and embeddable key/value store and cache.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name string

	// These values is useful to control operation status.
	bootstrapped int32
	// numMembers is used to check cluster quorum.
	numMembers int32

	// Number of successfully passed checkpoints
	passedCheckpoints int32

	// Currently owned partition count. Approximate LRU implementation
	// uses that.
	ownedPartitionCount uint64

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
	operations map[protocol.OpCode]func(*protocol.Message) *protocol.Message

	// Internal TCP server and its client for peer-to-peer communication.
	client *transport.Client
	server *transport.Server

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
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))

	cfg := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}
	cc := &transport.ClientConfig{
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlivePeriod,
		MaxConn:     1024, // TODO: Make this configurable.
	}
	client := transport.NewClient(cc)
	ctx, cancel := context.WithCancel(context.Background())

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
		operations: make(map[protocol.OpCode]func(*protocol.Message) *protocol.Message),
		server:     transport.NewServer(c.BindAddr, c.BindPort, c.KeepAlivePeriod, flogger),
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

func (db *Olric) requestDispatcher(req *protocol.Message) *protocol.Message {
	// Check bootstrapping status
	// Exclude protocol.OpUpdateRouting. The node is bootstrapped by this operation.
	if req.Op != protocol.OpUpdateRouting {
		if err := db.checkOperationStatus(); err != nil {
			return db.prepareResponse(req, err)
		}
	}

	// Run the incoming command.
	opr, ok := db.operations[req.Op]
	if !ok {
		return db.prepareResponse(req, ErrUnknownOperation)
	}
	return opr(req)
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

	for {
		select {
		case <-time.After(10 * time.Millisecond):
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

func (db *Olric) registerOperations() {
	// Put
	db.operations[protocol.OpPut] = db.exPutOperation
	db.operations[protocol.OpPutEx] = db.exPutOperation
	db.operations[protocol.OpPutReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutExReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutIf] = db.exPutOperation
	db.operations[protocol.OpPutIfEx] = db.exPutOperation
	db.operations[protocol.OpPutIfReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutIfExReplica] = db.putReplicaOperation

	// Get
	db.operations[protocol.OpGet] = db.exGetOperation
	db.operations[protocol.OpGetPrev] = db.getPrevOperation
	db.operations[protocol.OpGetBackup] = db.getBackupOperation

	// Delete
	db.operations[protocol.OpDelete] = db.exDeleteOperation
	db.operations[protocol.OpDeleteBackup] = db.deleteBackupOperation
	db.operations[protocol.OpDeletePrev] = db.deletePrevOperation

	// Lock/Unlock
	db.operations[protocol.OpLockWithTimeout] = db.exLockWithTimeoutOperation
	db.operations[protocol.OpLock] = db.exLockOperation
	db.operations[protocol.OpUnlock] = db.exUnlockOperation

	// Destroy
	db.operations[protocol.OpDestroy] = db.exDestroyOperation
	db.operations[protocol.OpDestroyDMap] = db.destroyDMapOperation

	// Atomic
	db.operations[protocol.OpIncr] = db.exIncrDecrOperation
	db.operations[protocol.OpDecr] = db.exIncrDecrOperation
	db.operations[protocol.OpGetPut] = db.exGetPutOperation

	// Pipeline
	db.operations[protocol.OpPipeline] = db.pipelineOperation

	// Expire
	db.operations[protocol.OpExpire] = db.exExpireOperation
	db.operations[protocol.OpExpireReplica] = db.expireReplicaOperation

	// Internal
	db.operations[protocol.OpUpdateRouting] = db.updateRoutingOperation
	db.operations[protocol.OpMoveDMap] = db.moveDMapOperation
	db.operations[protocol.OpLengthOfPart] = db.keyCountOnPartOperation

	// Aliveness
	db.operations[protocol.OpPing] = db.pingOperation

	// Node Stats
	db.operations[protocol.OpStats] = db.statsOperation

	// Distributed Query
	db.operations[protocol.OpLocalQuery] = db.localQueryOperation
	db.operations[protocol.OpQuery] = db.exQueryOperation
}

func (db *Olric) prepareResponse(req *protocol.Message, err error) *protocol.Message {
	switch {
	case err == nil:
		return req.Success()
	case err == ErrWriteQuorum:
		return req.Error(protocol.StatusErrWriteQuorum, err)
	case err == ErrReadQuorum:
		return req.Error(protocol.StatusErrReadQuorum, err)
	case err == ErrNoSuchLock:
		return req.Error(protocol.StatusErrNoSuchLock, err)
	case err == ErrLockNotAcquired:
		return req.Error(protocol.StatusErrLockNotAcquired, err)
	case err == ErrKeyNotFound, err == storage.ErrKeyNotFound:
		return req.Error(protocol.StatusErrKeyNotFound, err)
	case err == storage.ErrKeyTooLarge:
		return req.Error(protocol.StatusBadRequest, err)
	case err == ErrOperationTimeout:
		return req.Error(protocol.StatusErrOperationTimeout, err)
	case err == ErrKeyFound:
		return req.Error(protocol.StatusErrKeyFound, err)
	case err == ErrClusterQuorum:
		return req.Error(protocol.StatusErrClusterQuorum, err)
	case err == ErrUnknownOperation:
		return req.Error(protocol.StatusErrUnknownOperation, err)
	case err == ErrEndOfQuery:
		return req.Error(protocol.StatusErrEndOfQuery, err)
	default:
		return req.Error(protocol.StatusInternalServerError, err)
	}
}

func (db *Olric) requestTo(addr string, opcode protocol.OpCode, req *protocol.Message) (*protocol.Message, error) {
	resp, err := db.client.RequestTo(addr, opcode, req)
	if err != nil {
		return nil, err
	}

	switch {
	case resp.Status == protocol.StatusOK:
		return resp, nil
	case resp.Status == protocol.StatusInternalServerError:
		return nil, errors.Wrap(ErrInternalServerError, string(resp.Value))
	case resp.Status == protocol.StatusErrNoSuchLock:
		return nil, ErrNoSuchLock
	case resp.Status == protocol.StatusErrLockNotAcquired:
		return nil, ErrLockNotAcquired
	case resp.Status == protocol.StatusErrKeyNotFound:
		return nil, ErrKeyNotFound
	case resp.Status == protocol.StatusErrWriteQuorum:
		return nil, ErrWriteQuorum
	case resp.Status == protocol.StatusErrReadQuorum:
		return nil, ErrReadQuorum
	case resp.Status == protocol.StatusErrOperationTimeout:
		return nil, ErrOperationTimeout
	case resp.Status == protocol.StatusErrKeyFound:
		return nil, ErrKeyFound
	case resp.Status == protocol.StatusErrClusterQuorum:
		return nil, ErrClusterQuorum
	case resp.Status == protocol.StatusErrEndOfQuery:
		return nil, ErrEndOfQuery
	case resp.Status == protocol.StatusErrUnknownOperation:
		return nil, ErrUnknownOperation
	}
	return nil, fmt.Errorf("unknown status code: %d", resp.Status)
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

	ctx, cancel := context.WithTimeout(context.Background(), db.config.RequestTimeout)
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

// hostCmp returns true if o1 and o2 is the same.
func hostCmp(o1, o2 discovery.Member) bool {
	return o1.ID == o2.ID
}
