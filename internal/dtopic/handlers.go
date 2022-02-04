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

package dtopic

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) subscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	subscribeCmd, err := protocol.ParseSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, channel := range subscribeCmd.Channels {
		s.pubsub.Subscribe(conn, channel)
	}
}

func (s *Service) publishCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	publishCmd, err := protocol.ParsePublishCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	count := s.pubsub.Publish(publishCmd.Topic, publishCmd.Message)
	conn.WriteInt(count)
}

func (s *Service) psubscribeCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	psubscribeCmd, err := protocol.ParsePSubscribeCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	for _, topic := range psubscribeCmd.Patterns {
		s.pubsub.Psubscribe(conn, topic)
	}
}

func (s *Service) pubsubChannelsCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubChannelsCmd, err := protocol.ParsePubSubChannelsCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var channels []string
	if pubsubChannelsCmd.Pattern != "" {
		channels = s.pubsub.ChannelsWithPatterns(pubsubChannelsCmd.Pattern)
	} else {
		channels = s.pubsub.Channels()
	}
	conn.WriteArray(len(channels))
	for _, channel := range channels {
		conn.WriteBulkString(channel)
	}
}

func (s *Service) pubsubNumpatCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParsePubSubNumpatCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteInt(s.pubsub.Numpat())
}

func (s *Service) pubsubNumsubCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pubsubNumsubCmd, err := protocol.ParsePubSubNumsubCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if len(pubsubNumsubCmd.Channels) == 0 {
		conn.WriteArray(0)
		return
	}

	conn.WriteArray(len(pubsubNumsubCmd.Channels) * 2)
	for _, channel := range pubsubNumsubCmd.Channels {
		conn.WriteBulkString(channel)
		conn.WriteInt(s.pubsub.Numsub(channel))
	}
}
