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

/*Package olricdb provides embeddable, in-memory and distributed key/value store.*/
package olricdb

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/buraksezer/consistent"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/logutils"
	"github.com/hashicorp/memberlist"
	"github.com/julienschmidt/httprouter"
)

// OlricDB represens an member in the cluster. All functions on the OlricDB structure are safe to call concurrently.
type OlricDB struct {
	this       host
	config     *Config
	logger     *log.Logger
	hasher     Hasher
	discovery  *discovery
	consistent *consistent.Consistent
	partitions map[uint64]*partition
	backups    map[uint64]*partition
	transport  *httpTransport
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	fsckMtx    sync.Mutex
	routingMtx sync.Mutex

	// To control non-bootstrapped OlricDB instance
	bctx    context.Context
	bcancel context.CancelFunc
}

type vdata struct {
	Value interface{}
	TTL   int64
}

type dmap struct {
	sync.RWMutex

	locker *locker
	// user's key/value pairs
	d map[uint64]vdata
}

type partition struct {
	sync.RWMutex

	id     uint64
	backup bool
	owners []host
	// map name > map
	m map[string]*dmap
}

// New creates a new OlricDB object, otherwise returns an error.
func New(c *Config) (*OlricDB, error) {
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
		c.Hasher = newDefaultHasher()
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
	db := &OlricDB{
		ctx:        ctx,
		cancel:     cancel,
		logger:     c.Logger,
		config:     c,
		hasher:     c.Hasher,
		consistent: consistent.New(nil, cfg),
		transport:  newHTTPTransport(ctx, c),
		partitions: make(map[uint64]*partition),
		backups:    make(map[uint64]*partition),
		bctx:       bctx,
		bcancel:    bcancel,
	}
	db.transport.db = db

	// Create all the partitions. It's read-only. No need for locking.
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.partitions[i] = &partition{id: i, m: make(map[string]*dmap)}
	}

	// Create all the backup partitions. It's read-only. No need for locking.
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.backups[i] = &partition{
			id:     i,
			backup: true,
			m:      make(map[string]*dmap),
		}
	}
	return db, nil
}

func (db *OlricDB) prepare() error {
	dsc, err := newDiscovery(db.config)
	if err != nil {
		return err
	}
	db.discovery = dsc

	eventCh := db.discovery.subscribeNodeEvents()
	db.discovery.join()
	this, err := db.discovery.findMember(db.config.Name)
	if err != nil {
		db.logger.Printf("[DEBUG] Failed to get this node in cluster: %v", err)
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
func (db *OlricDB) Start() error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- db.transport.start()
	}()

	if err := db.transport.checkAliveness(db.config.Server.Addr); err != nil {
		return err
	}

	if err := db.prepare(); err != nil {
		return err
	}
	db.wg.Add(2)
	go db.updateRoutingPeriodically()
	go db.evictKeysAtBackground()
	return <-errCh
}

// Shutdown stops background servers and leaves the cluster.
func (db *OlricDB) Shutdown(ctx context.Context) error {
	db.cancel()

	var result error
	cx := ctx
	_, ok := ctx.Deadline()
	if ok {
		done := make(chan struct{})
		go func() {
			db.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				result = multierror.Append(result, err)
			}
		case <-done:
		}
	} else {
		db.wg.Wait()
		cx = context.Background()
	}

	err := db.transport.server.Shutdown(cx)
	if err != nil {
		result = multierror.Append(result, err)
	}

	if db.discovery != nil {
		err = db.discovery.memberlist.Shutdown()
		if err != nil {
			result = multierror.Append(result, err)
		}
	}
	// The GC will flush all the data.
	db.partitions = nil
	db.backups = nil
	return result
}

func (db *OlricDB) getPartitionID(hkey uint64) uint64 {
	return hkey % db.config.PartitionCount
}

func (db *OlricDB) getPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.partitions[partID]
}

func (db *OlricDB) getBackupPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.backups[partID]
}

func (db *OlricDB) getBackupPartitionOwners(hkey uint64) []host {
	bpart := db.getBackupPartition(hkey)
	bpart.RLock()
	defer bpart.RUnlock()
	owners := append([]host{}, bpart.owners...)
	return owners
}

func (db *OlricDB) getPartitionOwners(hkey uint64) []host {
	part := db.getPartition(hkey)
	part.RLock()
	defer part.RUnlock()
	owners := append([]host{}, part.owners...)
	return owners
}

func (db *OlricDB) getHKey(name, key string) uint64 {
	return db.hasher.Sum64([]byte(name + key))
}

func (db *OlricDB) locateHKey(hkey uint64) (host, error) {
	<-db.bctx.Done()
	if db.bctx.Err() == context.DeadlineExceeded {
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

func (db *OlricDB) locateKey(name, key string) (host, uint64, error) {
	hkey := db.getHKey(name, key)
	member, err := db.locateHKey(hkey)
	if err != nil {
		return host{}, 0, err
	}
	return member, hkey, nil
}

func (db *OlricDB) getDMap(name string, hkey uint64) *dmap {
	part := db.getPartition(hkey)
	part.Lock()
	defer part.Unlock()

	dmp, ok := part.m[name]
	if ok {
		return dmp
	}
	dmp = &dmap{
		locker: newLocker(),
		d:      make(map[uint64]vdata),
	}
	part.m[name] = dmp
	return dmp
}

// hostCmp returns true if o1 and o2 is the same.
func hostCmp(o1, o2 host) bool {
	return o1.Name == o2.Name && o1.Birthdate == o2.Birthdate
}

func (db *OlricDB) getBackupDmap(name string, hkey uint64) *dmap {
	bpart := db.getBackupPartition(hkey)
	bpart.Lock()
	defer bpart.Unlock()

	dmp, ok := bpart.m[name]
	if ok {
		return dmp
	}
	dmp = &dmap{d: make(map[uint64]vdata)}
	bpart.m[name] = dmp
	return dmp
}

func printHKey(hkey uint64) string {
	return strconv.FormatUint(hkey, 10)
}

func readHKey(ps httprouter.Params) (uint64, error) {
	return strconv.ParseUint(ps.ByName("hkey"), 10, 64)
}
