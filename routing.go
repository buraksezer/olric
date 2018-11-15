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

package olric

import (
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
)

var maxBackupCount = 3

type route struct {
	Owners  []host
	Backups []host
}

type routing map[uint64]route

func calcMaxBackupCount(backupCount, memCount int) int {
	// maxBackupCount is 3 now. If the cluster's host count is less than three
	// re-calculate backupCount. Partition manager will rearrange backup hosts
	// of a partition when a node join or leave.
	if backupCount > maxBackupCount {
		backupCount = maxBackupCount
	}

	if memCount-1 < backupCount {
		backupCount = memCount - 1
	}
	return backupCount
}

func (db *Olric) processNodeEvent(event memberlist.NodeEvent) {
	if event.Event == memberlist.NodeJoin {
		mt, _ := db.discovery.DecodeMeta(event.Node.Meta)
		member := host{
			Name:         event.Node.Name,
			NodeMetadata: *mt,
		}
		db.consistent.Add(member)
		db.log.Printf("[DEBUG] Node joined: %s", member)
	} else if event.Event == memberlist.NodeLeave {
		db.consistent.Remove(event.Node.Name)
		db.log.Printf("[DEBUG] Node leaved: %s", event.Node.Name)
	} else {
		db.log.Printf("[ERROR] Unknown event received: %v", event)
	}
}

func (db *Olric) distributeBackups(partID uint64, rt routing, backupCount int) {
	backups, err := db.consistent.GetClosestNForPartition(int(partID), backupCount)
	if err != nil {
		db.log.Printf("[ERROR] Failed to calculate backups for partID: %d: %v", partID, err)
		return
	}

	bpart := db.backups[partID]
	bpart.Lock()
	defer bpart.Unlock()

	data := rt[partID]
	defer func() {
		rt[partID] = data
	}()
	if len(bpart.owners) == 0 {
		for _, backup := range backups {
			bpart.owners = append(bpart.owners, backup.(host))
		}
		data.Backups = bpart.owners
		return
	}

	// Here add the new partition owner.
	for _, backup := range backups {
		var exists bool
		for i, bkp := range bpart.owners {
			if hostCmp(bkp, backup.(host)) {
				exists = true
				// Remove it from the current position
				bpart.owners = append(bpart.owners[:i], bpart.owners[i+1:]...)
				// Append it again to head
				bpart.owners = append(bpart.owners, backup.(host))
				break
			}
		}
		if !exists {
			bpart.owners = append(bpart.owners, backup.(host))
		}
	}

	// FIXME: What if tmp is empty?
	// Prune dead nodes
	tmp := []host{}
	for _, backup := range bpart.owners {
		cur, err := db.discovery.findMember(backup.Name)
		if err != nil {
			db.log.Printf("[ERROR] Failed to find %s in the cluster: %v", backup, err)
			continue
		}
		if !hostCmp(backup, cur) {
			db.log.Printf("[WARN] One of the backup owners is probably re-joined: %s", cur)
			continue
		}
		tmp = append(tmp, cur)
	}
	// FIXME: What if tmp is empty?

	// Prune empty nodes
	tbackups := []host{}
	for _, backup := range tmp[:len(tmp)-backupCount] {
		if hostCmp(db.this, backup) {
			if atomic.LoadInt32(&bpart.count) != 0 {
				tbackups = append(tbackups, backup)
			}
			continue
		}
		req := &protocol.Message{
			Extra: protocol.IsPartEmptyExtra{PartID: partID},
		}
		_, err := db.requestTo(backup.String(), protocol.OpIsBackupEmpty, req)
		if err != nil {
			if err != errBackupNotEmpty {
				db.log.Printf("[ERROR] Failed to check dmaps in partition backup: %d: %v", partID, err)
			}
			tbackups = append(tbackups, backup)
		}
	}
	tbackups = append(tbackups, tmp[len(tmp)-backupCount:]...)
	bpart.owners = tbackups
	data.Backups = bpart.owners
}

func (db *Olric) distributePrimaryCopies(partID uint64, rt routing) {
	owner := db.consistent.GetPartitionOwner(int(partID))
	part := db.partitions[partID]
	part.Lock()
	defer part.Unlock()

	data := rt[partID]
	defer func() {
		rt[partID] = data
	}()

	if len(part.owners) == 0 {
		part.owners = append(part.owners, owner.(host))
		data.Owners = part.owners
		return
	}
	// Here add the new partition owner.
	var exists bool
	for i, own := range part.owners {
		if hostCmp(own, owner.(host)) {
			exists = true
			// Remove it from the current position
			part.owners = append(part.owners[:i], part.owners[i+1:]...)
			// Append it again to head
			part.owners = append(part.owners, owner.(host))
			break
		}
	}
	if !exists {
		part.owners = append(part.owners, owner.(host))
	}

	// Prune dead nodes
	tmp := []host{}
	for _, own := range part.owners {
		cur, err := db.discovery.findMember(own.Name)
		if err != nil {
			db.log.Printf("[ERROR] Failed to find %s in the cluster: %v", own, err)
			continue
		}
		if !hostCmp(own, cur) {
			db.log.Printf("[WARN] One of the partitions owners is probably re-joined: %s", cur)
			continue
		}
		tmp = append(tmp, cur)
	}
	// Prune empty nodes
	owners := []host{}
	for _, own := range tmp[:len(tmp)-1] {
		if hostCmp(db.this, own) {
			if atomic.LoadInt32(&part.count) != 0 {
				owners = append(owners, own)
			}
			continue
		}
		req := &protocol.Message{
			Extra: protocol.IsPartEmptyExtra{PartID: partID},
		}
		_, err := db.requestTo(own.String(), protocol.OpIsPartEmpty, req)
		if err != nil {
			if err != errPartNotEmpty {
				db.log.Printf("[ERROR] Failed to check dmaps in partition: %d: %v", partID, err)
			}
			owners = append(owners, own)
		}
	}
	owners = append(owners, tmp[len(tmp)-1])
	part.owners = owners
	data.Owners = part.owners
}

func (db *Olric) distributePartitions() routing {
	rt := make(routing)
	memCount := len(db.consistent.GetMembers())
	backupCount := calcMaxBackupCount(db.config.BackupCount, memCount)
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		db.distributePrimaryCopies(partID, rt)
		if db.config.BackupCount != 0 && backupCount != 0 {
			db.distributeBackups(partID, rt, backupCount)
		}
	}
	return rt
}

func (db *Olric) updateRoutingOnCluster(rt routing) error {
	data, err := msgpack.Marshal(rt)
	if err != nil {
		return nil
	}

	var g errgroup.Group
	for _, member := range db.consistent.GetMembers() {
		mem := member.(host)
		if hostCmp(mem, db.this) {
			continue
		}
		g.Go(func() error {
			msg := &protocol.Message{
				Value: data,
			}
			_, err := db.requestTo(mem.String(), protocol.OpUpdateRouting, msg)
			return err
		})
	}
	return g.Wait()
}

func (db *Olric) updateRouting() {
	if !db.discovery.isCoordinator() {
		return
	}
	db.routingMx.Lock()
	defer db.routingMx.Unlock()
	pm := db.distributePartitions()
	err := db.updateRoutingOnCluster(pm)
	if err != nil {
		db.log.Printf("[ERROR] Failed to update routing table on cluster: %v", err)
	}
	db.fsck()
}

func (db *Olric) listenMemberlistEvents(eventCh chan memberlist.NodeEvent) {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			return
		case evt := <-eventCh:
			db.processNodeEvent(evt)
			db.updateRouting()
		}
	}
}

func (db *Olric) updateRoutingPeriodically() {
	defer db.wg.Done()
	// TODO: Make this parametric.
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.updateRouting()
		}
	}
}

func (db *Olric) updateRoutingOperation(req *protocol.Message) *protocol.Message {
	rt := make(routing)
	err := msgpack.Unmarshal(req.Value, &rt)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	for partID, data := range rt {
		// Set partition(primary copies) owners
		part := db.partitions[partID]
		part.Lock()
		part.owners = data.Owners
		part.Unlock()

		// Set backup owners
		bpart := db.backups[partID]
		bpart.Lock()
		bpart.owners = data.Backups
		bpart.Unlock()
	}

	// Bootstrapped by the coordinator.
	db.bcancel()
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		db.fsck()
	}()
	return req.Success()
}

func (db *Olric) isPartEmptyOperation(req *protocol.Message) *protocol.Message {
	partID := req.Extra.(protocol.IsPartEmptyExtra).PartID
	part := db.partitions[partID]
	if atomic.LoadInt32(&part.count) == 0 {
		return req.Success()
	}
	return req.Error(protocol.StatusPartNotEmpty, "")
}

func (db *Olric) isBackupEmptyOperation(req *protocol.Message) *protocol.Message {
	partID := req.Extra.(protocol.IsPartEmptyExtra).PartID
	part := db.backups[partID]

	if atomic.LoadInt32(&part.count) == 0 {
		return req.Success()
	}
	return req.Error(protocol.StatusBackupNotEmpty, "")
}
