package olric

import "time"

type ClusterTopologyEvent struct {
	Event     string
	Name      string
	Timestamp time.Time
}
