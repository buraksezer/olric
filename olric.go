// Copyright 2018-2019 Burak Sezer
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
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
const ReleaseVersion string = "0.2.0-rc.1"

const (
	nilTimeout                = 0 * time.Second
	requiredCheckpoints int32 = 2
)

// Olric implements a distributed, in-memory and embeddable key/value store and cache.
type Olric struct {
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

// cache keeps cache control parameters and access-log for keys in a DMap.
type cache struct {
	sync.RWMutex // protects accessLog

	maxIdleDuration time.Duration
	ttlDuration     time.Duration
	maxKeys         int
	maxInuse        int
	accessLog       map[uint64]int64
	lruSamples      int
	evictionPolicy  config.EvictionPolicy
}

// dmap defines the internal representation of a DMap.
type dmap struct {
	sync.RWMutex

	cache   *cache
	storage *storage.Storage
}

// partition is a basic, logical storage unit in Olric and stores DMaps in a sync.Map.
type partition struct {
	sync.RWMutex

	id     uint64
	backup bool
	m      sync.Map
	owners atomic.Value
}

// owner returns partition owner. It's not thread-safe.
func (p *partition) owner() discovery.Member {
	if p.backup {
		// programming error. it cannot occur at production!
		panic("cannot call this if backup is true")
	}
	owners := p.owners.Load().([]discovery.Member)
	if len(owners) == 0 {
		panic("owners list cannot be empty")
	}
	return owners[len(owners)-1]
}

// ownerCount returns the current owner count of a partition.
func (p *partition) ownerCount() int {
	owners := p.owners.Load()
	if owners == nil {
		return 0
	}
	return len(owners.([]discovery.Member))
}

// loadOwners loads the partition owners from atomic.Value and returns.
func (p *partition) loadOwners() []discovery.Member {
	owners := p.owners.Load()
	if owners == nil {
		return []discovery.Member{}
	}
	return owners.([]discovery.Member)
}

func (p *partition) length() int {
	var length int
	p.m.Range(func(_, dm interface{}) bool {
		d := dm.(*dmap)
		d.RLock()
		defer d.RUnlock()

		length += d.storage.Len()
		// Continue scanning.
		return true
	})
	return length
}

// DMap represents a distributed map instance.
type DMap struct {
	name string
	db   *Olric
}

// NewDMap creates an returns a new DMap instance.
func (db *Olric) NewDMap(name string) (*DMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := db.checkOperationStatus(); err != nil {
		return nil, err
	}
	return &DMap{
		name: name,
		db:   db,
	}, nil
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
		server:     transport.NewServer(c.Name, flogger, c.KeepAlivePeriod),
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

	table, err := db.distributePartitions()
	if err != nil {
		// This may be an consistent.ErrInsufficientMemberCount
		return err
	}
	_, err = db.updateRoutingTableOnCluster(table)
	if err == nil {
		// The coordinator bootstraps itself.
		atomic.StoreInt32(&db.bootstrapped, 1)
		db.log.V(2).Printf("[INFO] The cluster coordinator has been bootstrapped")
	}
	return err
}

// startDiscovery initializes and starts discovery subsystem.
func (db *Olric) startDiscovery() error {
	d := discovery.New(db.log, db.config)
	err := d.Start()
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
			db.log.V(2).Printf("[INFO] Join completed. Synced with %d initial nodes: %v", n, d.NumMembers())
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

	this, err := db.discovery.FindMemberByName(db.config.Name)
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

	db.log.V(2).Printf("[INFO] AdvertiseAddr: %s, AdvertisePort: %d",
		db.config.MemberlistConfig.AdvertiseAddr,
		db.config.MemberlistConfig.AdvertisePort)
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
	var name string
	if db.this.String() != "" {
		name = db.this.String()
	} else {
		name = db.config.Name
	}
	db.log.V(2).Printf("[INFO] %s is gone", name)
	return result
}

// getPartitionID returns partitionID for a given hkey.
func (db *Olric) getPartitionID(hkey uint64) uint64 {
	return hkey % db.config.PartitionCount
}

// getPartition loads the owner partition for a given hkey.
func (db *Olric) getPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.partitions[partID]
}

// getBackupPartition loads the backup partition for a given hkey.
func (db *Olric) getBackupPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.backups[partID]
}

// getBackupOwners returns the backup owners list for a given hkey.
func (db *Olric) getBackupPartitionOwners(hkey uint64) []discovery.Member {
	part := db.getBackupPartition(hkey)
	return part.owners.Load().([]discovery.Member)
}

// getPartitionOwners loads the partition owners list for a given hkey.
func (db *Olric) getPartitionOwners(hkey uint64) []discovery.Member {
	part := db.getPartition(hkey)
	return part.owners.Load().([]discovery.Member)
}

// getHKey returns hash-key, a.k.a hkey, for a key on a DMap.
func (db *Olric) getHKey(name, key string) uint64 {
	tmp := name + key
	return db.hasher.Sum64(*(*[]byte)(unsafe.Pointer(&tmp)))
}

// findPartitionOwner finds the partition owner for a key on a DMap.
func (db *Olric) findPartitionOwner(name, key string) (discovery.Member, uint64) {
	hkey := db.getHKey(name, key)
	return db.getPartition(hkey).owner(), hkey
}

func (db *Olric) setCacheConfiguration(dm *dmap, name string) error {
	// Try to set cache configuration for this DMap.
	dm.cache = &cache{}
	dm.cache.maxIdleDuration = db.config.Cache.MaxIdleDuration
	dm.cache.ttlDuration = db.config.Cache.TTLDuration
	dm.cache.maxKeys = db.config.Cache.MaxKeys
	dm.cache.maxInuse = db.config.Cache.MaxInuse
	dm.cache.lruSamples = db.config.Cache.LRUSamples
	dm.cache.evictionPolicy = db.config.Cache.EvictionPolicy

	if db.config.Cache.DMapConfigs != nil {
		// config.DMapCacheConfig struct can be used for fine-grained control.
		c, ok := db.config.Cache.DMapConfigs[name]
		if ok {
			if dm.cache.maxIdleDuration != c.MaxIdleDuration {
				dm.cache.maxIdleDuration = c.MaxIdleDuration
			}
			if dm.cache.ttlDuration != c.TTLDuration {
				dm.cache.ttlDuration = c.TTLDuration
			}
			if dm.cache.evictionPolicy != c.EvictionPolicy {
				dm.cache.evictionPolicy = c.EvictionPolicy
			}
			if dm.cache.maxKeys != c.MaxKeys {
				dm.cache.maxKeys = c.MaxKeys
			}
			if dm.cache.maxInuse != c.MaxInuse {
				dm.cache.maxInuse = c.MaxInuse
			}
			if dm.cache.lruSamples != c.LRUSamples {
				dm.cache.lruSamples = c.LRUSamples
			}
			if dm.cache.evictionPolicy != c.EvictionPolicy {
				dm.cache.evictionPolicy = c.EvictionPolicy
			}
		}
	}

	if dm.cache.evictionPolicy == config.LRUEviction || dm.cache.maxIdleDuration != 0 {
		dm.cache.accessLog = make(map[uint64]int64)
	}

	// TODO: Create a new function to verify cache config.
	if dm.cache.evictionPolicy == config.LRUEviction {
		if dm.cache.maxInuse <= 0 && dm.cache.maxKeys <= 0 {
			return fmt.Errorf("maxInuse or maxKeys have to be greater than zero")
		}
		// set the default value.
		if dm.cache.lruSamples == 0 {
			dm.cache.lruSamples = config.DefaultLRUSamples
		}
	}
	return nil
}

// createDMap creates and returns a new dmap, internal representation of a DMap.
func (db *Olric) createDMap(part *partition, name string, str *storage.Storage) (*dmap, error) {
	// We need to protect storage.New
	part.Lock()
	defer part.Unlock()

	// Try to load one more time. Another goroutine may have created the dmap.
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}

	// create a new map here.
	nm := &dmap{
		storage: str,
	}

	if db.config.Cache != nil {
		err := db.setCacheConfiguration(nm, name)
		if err != nil {
			return nil, err
		}
	}

	// rebalancer code may send a storage instance for the new DMap. Just use it.
	if nm.storage != nil {
		nm.storage = str
	} else {
		nm.storage = storage.New(db.config.TableSize)
	}

	part.m.Store(name, nm)
	return nm, nil
}

func (db *Olric) getOrCreateDMap(part *partition, name string) (*dmap, error) {
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name, nil)
}

// getDMap loads or creates a dmap.
func (db *Olric) getDMap(name string, hkey uint64) (*dmap, error) {
	part := db.getPartition(hkey)
	return db.getOrCreateDMap(part, name)
}

func (db *Olric) getBackupDMap(name string, hkey uint64) (*dmap, error) {
	part := db.getBackupPartition(hkey)
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name, nil)
}

// hostCmp returns true if o1 and o2 is the same.
func hostCmp(o1, o2 discovery.Member) bool {
	return o1.ID == o2.ID
}

func (db *Olric) prepareResponse(req *protocol.Message, err error) *protocol.Message {
	if err == nil {
	}
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
