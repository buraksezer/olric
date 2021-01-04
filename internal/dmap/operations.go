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
	operations[protocol.OpPut] = s.exPutOperation
	operations[protocol.OpPutEx] = s.exPutOperation
	operations[protocol.OpPutReplica] = s.putReplicaOperation
	operations[protocol.OpPutExReplica] = s.putReplicaOperation
	operations[protocol.OpPutIf] = s.exPutOperation
	operations[protocol.OpPutIfEx] = s.exPutOperation
	operations[protocol.OpPutIfReplica] = s.putReplicaOperation
	operations[protocol.OpPutIfExReplica] = s.putReplicaOperation

	// DMap.Get
	operations[protocol.OpGet] = s.exGetOperation
	operations[protocol.OpGetPrev] = s.getPrevOperation
	operations[protocol.OpGetBackup] = s.getBackupOperation

	// DMap.Delete
	operations[protocol.OpDelete] = s.exDeleteOperation
	operations[protocol.OpDeleteBackup] = s.deleteBackupOperation
	operations[protocol.OpDeletePrev] = s.deletePrevOperation

	// DMap.Atomic
	operations[protocol.OpIncr] = s.exIncrDecrOperation
	operations[protocol.OpDecr] = s.exIncrDecrOperation
	operations[protocol.OpGetPut] = s.exGetPutOperation

	// DMap.Destroy
	operations[protocol.OpDestroy] = s.exDestroyOperation
	operations[protocol.OpDestroyDMap] = s.destroyDMapOperation


	// DMap.Lock
	operations[protocol.OpLockWithTimeout] = s.exLockWithTimeoutOperation
	operations[protocol.OpLock] = s.exLockOperation

	// DMap.Unlock
	operations[protocol.OpUnlock] = s.exUnlockOperation

	// DMap.Atomic
	operations[protocol.OpIncr] = s.exIncrDecrOperation
	operations[protocol.OpDecr] = s.exIncrDecrOperation
	operations[protocol.OpGetPut] = s.exGetPutOperation

	// DMap.Expire
	operations[protocol.OpExpire] = s.exExpireOperation
	operations[protocol.OpExpireReplica] = s.expireReplicaOperation

	// DMap.Query (distributed query)
	operations[protocol.OpLocalQuery] = s.localQueryOperation
	operations[protocol.OpQuery] = s.exQueryOperation
}
