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

package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/tidwall/redcon"
)

func (s *Service) putOperationCommon(w, r protocol.EncodeDecoder, f func(dm *DMap, r protocol.EncodeDecoder) error) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	err = f(dm, r)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) putOperation(w, r protocol.EncodeDecoder) {
	s.putOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) error {
		e := newEnvFromReq(r, partitions.PRIMARY)
		return dm.put(e)
	})
}

func (s *Service) putCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	putCmd, err := resp.ParsePutCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(putCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	var options []PutOption
	switch {
	case putCmd.NX:
		options = append(options, NX())
	case putCmd.XX:
		options = append(options, XX())
	case putCmd.EX != 0:
		options = append(options, EX(time.Duration(putCmd.EX*float64(time.Second))))
	case putCmd.PX != 0:
		options = append(options, PX(time.Duration(putCmd.PX*int64(time.Millisecond))))
	case putCmd.EXAT != 0:
		options = append(options, EXAT(time.Duration(putCmd.EXAT*float64(time.Second))))
	case putCmd.PXAT != 0:
		options = append(options, PXAT(time.Duration(putCmd.PXAT*int64(time.Millisecond))))
	}

	var pc putConfig
	for _, opt := range options {
		opt(&pc)
	}
	e := &env{
		putConfig: &pc,
		putCmd:    putCmd, // this is good if we want to reconstruct the protocol message
		kind:      partitions.PRIMARY,
		dmap:      dm.name,
		key:       putCmd.Key,
		value:     putCmd.Value,
	}
	err = dm.put(e)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteString(resp.StatusOK)
}

func (s *Service) putReplicaCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	putReplicaCmd, err := resp.ParsePutReplicaCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(putReplicaCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	e := &env{
		dmap:  putReplicaCmd.DMap,
		key:   putReplicaCmd.Key,
		value: putReplicaCmd.Value,
	}
	err = dm.putOnReplicaFragment(e)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteString(resp.StatusOK)
}

func (s *Service) putReplicaOperation(w, r protocol.EncodeDecoder) {
	s.putOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) error {
		e := newEnvFromReq(r, partitions.BACKUP)
		return dm.putOnReplicaFragment(e)
	})
}
