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

package events

const (
	NodeJoinEvent   = "join"
	NodeLeaveEvent  = "leave"
	NodeUpdateEvent = "update"
)

const (
	ClusterEventsTopic = "olric.internal.cluster_events"
)

type Event struct {
	Message       interface{}
	PublisherAddr string
	PublishedAt   int64
}

type ClusterEvent struct {
	Event string

	// Name is name of the node in the cluster.
	Name string

	// ID is the unique identifier of this node in the cluster. It's derived
	// from Name and Birthdate.
	ID uint64

	// Birthdate is UNIX time in nanoseconds.
	Birthdate int64
}
