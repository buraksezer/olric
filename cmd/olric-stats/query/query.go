// Copyright 2019 Burak Sezer
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

package query

import (
	"encoding/json"
	"fmt"
	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/serializer"
	"github.com/buraksezer/olric/stats"
	"log"
	"time"
)

type Query struct {
	addr   string
	client *client.Client
	log    *log.Logger
}

func New(addr, timeout string, logger *log.Logger) (*Query, error) {
	dt, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}
	cc := &client.Config{
		Addrs:       []string{addr},
		Serializer:  serializer.NewMsgpackSerializer(),
		DialTimeout: dt,
		MaxConn:     1,
	}
	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}
	return &Query{
		addr:   addr,
		client: c,
		log:    logger,
	}, nil
}

func (q *Query) prettyPrint(partID uint64, part stats.Partition) {
	q.log.Printf("PartID: %d", partID)
	if len(part.Owner.String()) == 0 {
		q.log.Printf("  Owner: not found")
	} else {
		q.log.Printf("  Owner: %s", part.Owner.String())
	}
	if len(part.PreviousOwners) != 0 {
		q.log.Printf("  Previous Owners:")
		for idx, previous := range part.PreviousOwners {
			q.log.Printf("  %d: %s", idx, previous.Name)
		}
	} else {
		q.log.Printf("  Previous Owners: not found")
	}
	if len(part.Backups) != 0 {
		q.log.Printf("  Backups:")
		for idx, backup := range part.Backups {
			q.log.Printf("    %d: %s", idx+1, backup.Name)
		}
	} else {
		q.log.Printf("  Backups: not found")
	}
	q.log.Printf("  Key count: %d", part.KeyCount)
	if len(part.DMaps) != 0 {
		q.log.Printf("  DMaps:")
		for name, dm := range part.DMaps {
			q.log.Printf("    Name: %s", name)
			q.log.Printf("    Length: %d", dm.Length)
			q.log.Printf("    Allocated: %d", dm.SlabInfo.Allocated)
			q.log.Printf("    Inuse: %d", dm.SlabInfo.Inuse)
			q.log.Printf("    Garbage: %d", dm.SlabInfo.Garbage)
			q.log.Printf("\n")
		}
	} else {
		q.log.Printf("  DMaps: not found")
	}
	q.log.Printf("  Key count: %d", part.TotalKeyCount)
	q.log.Printf("\n")
}

func (q *Query) PrintRawStats(backup bool) error {
	data, err := q.client.Stats(q.addr)
	if err != nil {
		return err
	}
	var totalKeyCount int
	var totalInuse int
	var totalAllocated int
	var totalGarbage int
	partitions := data.Partitions
	if backup {
		partitions = data.Backups
	}
	for partID := uint64(0); partID < uint64(len(partitions)); partID++ {
		if part, ok := partitions[partID]; ok {
			q.prettyPrint(partID, part)
			totalKeyCount += part.TotalKeyCount
			for _, dm := range part.DMaps {
				totalInuse += dm.SlabInfo.Inuse
				totalAllocated += dm.SlabInfo.Allocated
				totalGarbage += dm.SlabInfo.Garbage
			}
		}
	}
	q.log.Printf("Summary for %s:\n\n", q.addr)
	q.log.Printf("Total key count: %d", totalKeyCount)
	q.log.Printf("Total partition count: %d", len(data.Partitions))
	q.log.Printf("Total Allocated: %d", totalAllocated)
	q.log.Printf("Total Inuse: %d", totalInuse)
	q.log.Printf("Total Garbage: %d", totalGarbage)
	return nil
}

func (q *Query) PrintPartitionStats(partID uint64, backup bool) error {
	data, err := q.client.Stats(q.addr)
	if err != nil {
		return err
	}

	partitions := data.Partitions
	if backup {
		partitions = data.Backups
	}
	part, ok := partitions[partID]
	if !ok {
		return fmt.Errorf("partition could not be found")
	}
	q.prettyPrint(partID, part)
	return nil
}

func (q *Query) Dump() error {
	data, err := q.client.Stats(q.addr)
	if err != nil {
		return err
	}
	js, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	q.log.Printf(string(js))
	return nil
}

func (q *Query) PrintRuntimeStats() error {
	data, err := q.client.Stats(q.addr)
	if err != nil {
		return err
	}

	js, err := json.MarshalIndent(data.Runtime, "", "  ")
	if err != nil {
		return err
	}
	q.log.Printf(string(js))
	return nil
}
