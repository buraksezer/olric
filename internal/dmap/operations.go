// Copyright 2018-2020 Burak Sezer
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

import "github.com/buraksezer/olric/internal/protocol"

func (s *Service) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
	// Operations on DMap data structure
	//
	// DMap.Put
	s.operations[protocol.OpPut] = s.exPutOperation
	s.operations[protocol.OpPutEx] = s.exPutOperation
	s.operations[protocol.OpPutReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutExReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutIf] = s.exPutOperation
	s.operations[protocol.OpPutIfEx] = s.exPutOperation
	s.operations[protocol.OpPutIfReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutIfExReplica] = s.putReplicaOperation

	// DMap.Get
	s.operations[protocol.OpGet] = s.exGetOperation
	s.operations[protocol.OpGetPrev] = s.getPrevOperation
	s.operations[protocol.OpGetBackup] = s.getBackupOperation

	// DMap.Delete
	s.operations[protocol.OpDelete] = s.exDeleteOperation
	s.operations[protocol.OpDeleteBackup] = s.deleteBackupOperation
	s.operations[protocol.OpDeletePrev] = s.deletePrevOperation

	// DMap.Atomic
	s.operations[protocol.OpIncr] = s.exIncrDecrOperation
	s.operations[protocol.OpDecr] = s.exIncrDecrOperation
	s.operations[protocol.OpGetPut] = s.exGetPutOperation

	// DMap.Destroy
	s.operations[protocol.OpDestroy] = s.exDestroyOperation
	s.operations[protocol.OpDestroyDMap] = s.destroyDMapOperation

	// DMap.Pipeline
	s.operations[protocol.OpPipeline] = s.pipelineOperation

	// DMap.Lock
	s.operations[protocol.OpLockWithTimeout] = s.exLockWithTimeoutOperation
	s.operations[protocol.OpLock] = s.exLockOperation

	// DMap.Unlock
	s.operations[protocol.OpUnlock] = s.exUnlockOperation

	// DMap.Atomic
	s.operations[protocol.OpIncr] = s.exIncrDecrOperation
	s.operations[protocol.OpDecr] = s.exIncrDecrOperation
	s.operations[protocol.OpGetPut] = s.exGetPutOperation

	// DMap.Expire
	s.operations[protocol.OpExpire] = s.exExpireOperation
	s.operations[protocol.OpExpireReplica] = s.expireReplicaOperation

	// DMap.Query (distributed query)
	s.operations[protocol.OpLocalQuery] = s.localQueryOperation
	s.operations[protocol.OpQuery] = s.exQueryOperation

	// Internals
	s.operations[protocol.OpMoveDMap] = s.moveDMapOperation

	// Merge
	for code, f := range s.operations {
		operations[code] = f
	}
}

func (s *Service) operationsCommon(w, r protocol.EncodeDecoder) {

}