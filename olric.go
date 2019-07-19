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
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/internal/transport"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/logutils"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotFound is returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrInternalServerError means that something unintentionally went wrong while processing the request.
	ErrInternalServerError = errors.New("internal server error")

	errPartNotEmpty   = errors.New("partition not empty")
	errBackupNotEmpty = errors.New("backup not empty")
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.1.0"

const nilTimeout = 0 * time.Second

var bootstrapTimeoutDuration = 10 * time.Second

// Olric implements a distributed, in-memory and embeddable key/value store.
type Olric struct {
	this       host
	config     *Config
	log        *log.Logger
	hasher     Hasher
	serializer Serializer
	discovery  *discovery
	consistent *consistent.Consistent
	partitions map[uint64]*partition
	backups    map[uint64]*partition
	client     *transport.Client
	server     *transport.Server
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	fsckMx     sync.Mutex
	routingMx  sync.Mutex
	// To control non-bootstrapped Olric instance
	bcx     context.Context
	bcancel context.CancelFunc
}

type cache struct {
	sync.RWMutex // protects accessLog

	maxIdleDuration time.Duration
	ttlDuration     time.Duration
	maxKeys         int
	accessLog       map[uint64]int64
	lruSamples      int
	evictionPolicy  EvictionPolicy
}

type dmap struct {
	sync.RWMutex

	locker *locker
	cache  *cache
	str    *storage.Storage
}

type partition struct {
	count  int32
	id     uint64
	backup bool
	m      sync.Map

	sync.RWMutex
	owners []host
}

// DMap represents a distributed map object.
type DMap struct {
	name string
	db   *Olric
}

// NewDMap creates an returns a new DMap object.
func (db *Olric) NewDMap(name string) *DMap {
	return &DMap{
		name: name,
		db:   db,
	}
}

// New creates a new Olric object, otherwise returns an error.
func New(c *Config) (*Olric, error) {
	if c == nil {
		c = &Config{}
	}
	if c.LogOutput != nil && c.Logger != nil {
		return nil, fmt.Errorf("cannot specify both LogOutput and Logger")
	}

	if c.Logger == nil {
		logDest := c.LogOutput
		if logDest == nil {
			logDest = os.Stderr
		}

		if c.LogLevel == "" {
			c.LogLevel = DefaultLogLevel
		}

		filter := &logutils.LevelFilter{
			Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
			MinLevel: logutils.LogLevel(c.LogLevel),
			Writer:   logDest,
		}
		c.Logger = log.New(logDest, "", log.LstdFlags)
		c.Logger.SetOutput(filter)
	}

	if c.Hasher == nil {
		c.Hasher = NewDefaultHasher()
	}
	if c.Serializer == nil {
		c.Serializer = NewGobSerializer()
	}
	if c.Name == "" {
		name, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		c.Name = name + ":0"
	}
	if c.LoadFactor == 0 {
		c.LoadFactor = DefaultLoadFactor
	}
	if c.PartitionCount == 0 {
		c.PartitionCount = DefaultPartitionCount
	}

	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}

	cfg := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}
	ctx, cancel := context.WithCancel(context.Background())
	bctx, bcancel := context.WithTimeout(context.Background(), bootstrapTimeoutDuration)

	cc := &transport.ClientConfig{
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlivePeriod,
		MaxConn:     1024, // TODO: Make this configurable.
	}
	client := transport.NewClient(cc)
	db := &Olric{
		ctx:        ctx,
		cancel:     cancel,
		log:        c.Logger,
		config:     c,
		hasher:     c.Hasher,
		serializer: c.Serializer,
		consistent: consistent.New(nil, cfg),
		client:     client,
		partitions: make(map[uint64]*partition),
		backups:    make(map[uint64]*partition),
		bcx:        bctx,
		bcancel:    bcancel,
		server:     transport.NewServer(c.Name, c.Logger, c.KeepAlivePeriod),
	}
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
	db.wg.Add(1)
	go db.updateCurrentUnixNano()
	return db, nil
}

func (db *Olric) startDiscovery() error {
	dsc, err := newDiscovery(db.config)
	if err != nil {
		return err
	}
	db.discovery = dsc

	eventCh := db.discovery.subscribeNodeEvents()
	db.discovery.join()
	this, err := db.discovery.findMember(db.config.Name)
	if err != nil {
		db.log.Printf("[DEBUG] Failed to get this node in cluster: %v", err)
		serr := db.discovery.shutdown()
		if serr != nil {
			return serr
		}
		return err
	}

	db.this = this
	db.consistent.Add(db.this)
	if db.discovery.isCoordinator() {
		db.distributePartitions()
		// The coordinator bootstraps itself.
		db.bcancel()
	}

	db.wg.Add(1)
	go db.listenMemberlistEvents(eventCh)
	return nil
}

// Start starts background servers and joins the cluster.
func (db *Olric) Start() error {
	errCh := make(chan error, 1)
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		// TODO: Check files on disk.
		if db.config.KeyFile != "" && db.config.CertFile != "" {
			errCh <- db.server.ListenAndServeTLS(db.config.CertFile, db.config.KeyFile)
			return
		}
		errCh <- db.server.ListenAndServe()
	}()

	<-db.server.StartCh
	select {
	case err := <-errCh:
		return err
	default:
	}

	if err := db.startDiscovery(); err != nil {
		return err
	}
	db.wg.Add(3)
	go db.updateRoutingPeriodically()
	go db.evictKeysAtBackground()
	go db.deleteStaleDMapsAtBackground()
	return <-errCh
}

func (db *Olric) registerOperations() {
	// Put
	db.server.RegisterOperation(protocol.OpPut, db.exPutOperation)
	db.server.RegisterOperation(protocol.OpPutEx, db.exPutExOperation)
	db.server.RegisterOperation(protocol.OpPutBackup, db.putBackupOperation)

	// Get
	db.server.RegisterOperation(protocol.OpGet, db.exGetOperation)
	db.server.RegisterOperation(protocol.OpGetPrev, db.getPrevOperation)
	db.server.RegisterOperation(protocol.OpGetBackup, db.getBackupOperation)

	// Delete
	db.server.RegisterOperation(protocol.OpDelete, db.exDeleteOperation)
	db.server.RegisterOperation(protocol.OpDeleteBackup, db.deleteBackupOperation)
	db.server.RegisterOperation(protocol.OpDeletePrev, db.deletePrevOperation)

	// Lock/Unlock
	db.server.RegisterOperation(protocol.OpLockWithTimeout, db.exLockWithTimeoutOperation)
	db.server.RegisterOperation(protocol.OpUnlock, db.exUnlockOperation)
	db.server.RegisterOperation(protocol.OpFindLock, db.findLockOperation)
	db.server.RegisterOperation(protocol.OpLockPrev, db.lockPrevOperation)
	db.server.RegisterOperation(protocol.OpUnlockPrev, db.unlockPrevOperation)

	// Destroy
	db.server.RegisterOperation(protocol.OpDestroy, db.exDestroyOperation)
	db.server.RegisterOperation(protocol.OpDestroyDMap, db.destroyDMapOperation)

	// Atomic
	db.server.RegisterOperation(protocol.OpIncr, db.exIncrDecrOperation)
	db.server.RegisterOperation(protocol.OpDecr, db.exIncrDecrOperation)
	db.server.RegisterOperation(protocol.OpGetPut, db.exGetPutOperation)

	// Internal
	db.server.RegisterOperation(protocol.OpUpdateRouting, db.updateRoutingOperation)
	db.server.RegisterOperation(protocol.OpMoveDMap, db.moveDMapOperation)
	db.server.RegisterOperation(protocol.OpBackupMoveDMap, db.moveBackupDMapOperation)
	db.server.RegisterOperation(protocol.OpIsPartEmpty, db.isPartEmptyOperation)
	db.server.RegisterOperation(protocol.OpIsBackupEmpty, db.isBackupEmptyOperation)
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	db.cancel()

	var result error
	if err := db.server.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	if db.discovery != nil {
		err := db.discovery.memberlist.Shutdown()
		if err != nil {
			result = multierror.Append(result, err)
		}
	}

	db.wg.Wait()

	// The GC will flush all the data.
	db.partitions = nil
	db.backups = nil
	return result
}

func (db *Olric) getPartitionID(hkey uint64) uint64 {
	return hkey % db.config.PartitionCount
}

func (db *Olric) getPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.partitions[partID]
}

func (db *Olric) getBackupPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.backups[partID]
}

func (db *Olric) getBackupPartitionOwners(hkey uint64) []host {
	bpart := db.getBackupPartition(hkey)
	bpart.RLock()
	defer bpart.RUnlock()
	owners := append([]host{}, bpart.owners...)
	return owners
}

func (db *Olric) getPartitionOwners(hkey uint64) []host {
	part := db.getPartition(hkey)
	part.RLock()
	defer part.RUnlock()
	owners := append([]host{}, part.owners...)
	return owners
}

func (db *Olric) getHKey(name, key string) uint64 {
	tmp := name + key
	return db.hasher.Sum64(*(*[]byte)(unsafe.Pointer(&tmp)))
}

func (db *Olric) locateHKey(hkey uint64) (host, error) {
	<-db.bcx.Done()
	if db.bcx.Err() == context.DeadlineExceeded {
		return host{}, ErrOperationTimeout
	}

	part := db.getPartition(hkey)
	part.RLock()
	defer part.RUnlock()
	if len(part.owners) == 0 {
		return host{}, fmt.Errorf("no owner found for hkey: %d", hkey)
	}
	return part.owners[len(part.owners)-1], nil
}

func (db *Olric) locateKey(name, key string) (host, uint64, error) {
	hkey := db.getHKey(name, key)
	member, err := db.locateHKey(hkey)
	if err != nil {
		return host{}, 0, err
	}
	return member, hkey, nil
}

func (db *Olric) setCacheConfiguration(dm *dmap, name string) {
	// Try to set cache configuration for this DMap.
	dm.cache = &cache{}
	dm.cache.maxIdleDuration = db.config.Cache.MaxIdleDuration
	dm.cache.ttlDuration = db.config.Cache.TTLDuration
	dm.cache.maxKeys = db.config.Cache.MaxKeys / int(db.config.PartitionCount)
	dm.cache.lruSamples = db.config.Cache.LRUSamples
	dm.cache.evictionPolicy = db.config.Cache.EvictionPolicy

	if db.config.Cache.DMapConfigs != nil {
		// config.DMapConfig struct can be used for fine-grained control.
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
				dm.cache.maxKeys = c.MaxKeys / int(db.config.PartitionCount)
			}
			if dm.cache.lruSamples != c.LRUSamples {
				dm.cache.lruSamples = c.LRUSamples
			}
		}
	}

	if dm.cache.evictionPolicy == LRUEviction || dm.cache.maxIdleDuration != 0 {
		dm.cache.accessLog = make(map[uint64]int64)
	}
	// set the default value.
	if dm.cache.lruSamples == 0 {
		dm.cache.lruSamples = DefaultLRUSamples
	}
}

func (db *Olric) createDMap(part *partition, name string, str *storage.Storage) (*dmap, error) {
	// We need to protect snapshot.RegisterDMap and storage.New
	part.Lock()
	defer part.Unlock()

	// Try to load one more time. Another goroutine may have created the dmap.
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}

	// create a new map here.
	nm := &dmap{
		str: str,
	}

	// fsck code may send a strorage instange for the new DMap. Just use it.
	if nm.str != nil {
		nm.str = str
	} else {
		nm.str = storage.New(0)
	}

	if !part.backup {
		// Create this on the owners, not backups.
		nm.locker = newLocker()
	}

	if db.config.Cache != nil {
		db.setCacheConfiguration(nm, name)
	}
	part.m.Store(name, nm)
	atomic.AddInt32(&part.count, 1)
	return nm, nil
}

func (db *Olric) getDMap(name string, hkey uint64) (*dmap, error) {
	part := db.getPartition(hkey)
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name, nil)
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
func hostCmp(o1, o2 host) bool {
	return o1.Name == o2.Name && o1.Birthdate == o2.Birthdate
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
	case resp.Status == protocol.StatusNoSuchLock:
		return nil, ErrNoSuchLock
	case resp.Status == protocol.StatusKeyNotFound:
		return nil, ErrKeyNotFound
	case resp.Status == protocol.StatusPartNotEmpty:
		return nil, errPartNotEmpty
	case resp.Status == protocol.StatusBackupNotEmpty:
		return nil, errBackupNotEmpty
	}
	return nil, fmt.Errorf("unknown status code: %d", resp.Status)
}

var currentUnixNano int64

// updates currentUnixNano 10 times per second. This is better than getting current time
// for every request. It has its own cost.
func (db *Olric) updateCurrentUnixNano() {
	defer db.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			atomic.StoreInt64(&currentUnixNano, time.Now().UnixNano())
		case <-db.ctx.Done():
			return
		}
	}
}

func getTTL(timeout time.Duration) int64 {
	// convert nanoseconds to milliseconds
	return (timeout.Nanoseconds() + atomic.LoadInt64(&currentUnixNano)) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	// convert nanoseconds to milliseconds
	return (atomic.LoadInt64(&currentUnixNano) / 1000000) >= ttl
}
