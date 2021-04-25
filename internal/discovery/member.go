// Copyright 2018-2021 Burak Sezer
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

package discovery

import (
	"encoding/binary"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/vmihailenco/msgpack"
)

// Member represents a node in the cluster.
type Member struct {
	Name      string
	NameHash  uint64
	ID        uint64
	Birthdate int64
}

// CompareByID returns true if two members denote the same member in the cluster.
func (m Member) CompareByID(other Member) bool {
	// ID variable is calculated by combining member's name and birthdate
	return m.ID == other.ID
}

// CompareByName returns true if the two members has the same name in the cluster.
// This function is intended to redirect the requests to the partition owner.
func (m Member) CompareByName(other Member) bool {
	return m.NameHash == other.NameHash
}

func (m Member) String() string {
	return m.Name
}

func (m Member) Encode() ([]byte, error) {
	return msgpack.Marshal(m)
}

func NewMemberFromMetadata(metadata []byte) (Member, error) {
	res := &Member{}
	err := msgpack.Unmarshal(metadata, res)
	return *res, err
}

func NewMember(c *config.Config) Member {
	// Calculate member's identity. It's useful to compare hosts.
	birthdate := time.Now().UnixNano()

	buf := make([]byte, 8+len(c.MemberlistConfig.Name))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(c.MemberlistConfig.Name)...)
	nameHash := c.Hasher.Sum64([]byte(c.MemberlistConfig.Name))
	return Member{
		Name:      c.MemberlistConfig.Name,
		NameHash:  nameHash,
		ID:        c.Hasher.Sum64(buf),
		Birthdate: birthdate,
	}
}
