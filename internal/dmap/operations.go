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

import "github.com/buraksezer/olric/internal/protocol"

func (s *Service) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
	// Operations on DMap data structure
	//
	// DMap.Put
	s.operations[protocol.OpPut] = s.putOperation
	s.operations[protocol.OpPutEx] = s.putOperation
	s.operations[protocol.OpPutReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutExReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutIf] = s.putOperation
	s.operations[protocol.OpPutIfEx] = s.putOperation
	s.operations[protocol.OpPutIfReplica] = s.putReplicaOperation
	s.operations[protocol.OpPutIfExReplica] = s.putReplicaOperation

	// DMap.Get
	s.operations[protocol.OpGet] = s.getOperation
	s.operations[protocol.OpGetPrev] = s.getPrevOperation
	s.operations[protocol.OpGetReplica] = s.getReplicaOperation

	// DMap.Delete
	s.operations[protocol.OpDelete] = s.deleteOperation
	s.operations[protocol.OpDeleteReplica] = s.deleteReplicaOperation
	s.operations[protocol.OpDeletePrev] = s.deletePrevOperation

	// DMap.Atomic
	s.operations[protocol.OpIncr] = s.incrDecrOperation
	s.operations[protocol.OpDecr] = s.incrDecrOperation
	s.operations[protocol.OpGetPut] = s.getPutOperation

	// DMap.Destroy
	s.operations[protocol.OpDestroy] = s.destroyOperation
	s.operations[protocol.OpDestroyDMapInternal] = s.destroyDMapOperation

	// DMap.Pipeline
	s.operations[protocol.OpPipeline] = s.pipelineOperation

	// DMap.Lock
	s.operations[protocol.OpLockWithTimeout] = s.lockWithTimeoutOperation
	s.operations[protocol.OpLock] = s.lockOperation

	// DMap.Unlock
	s.operations[protocol.OpUnlock] = s.unlockOperation

	// DMap.Lease
	s.operations[protocol.OpLockLease] = s.leaseLockOperation

	// DMap.Atomic
	s.operations[protocol.OpIncr] = s.incrDecrOperation
	s.operations[protocol.OpDecr] = s.incrDecrOperation
	s.operations[protocol.OpGetPut] = s.getPutOperation

	// DMap.Expire
	s.operations[protocol.OpExpire] = s.expireOperation
	s.operations[protocol.OpExpireReplica] = s.expireReplicaOperation

	// DMap.Query (distributed query)
	s.operations[protocol.OpLocalQuery] = s.localQueryOperation
	s.operations[protocol.OpQuery] = s.queryOperation

	// Internals
	s.operations[protocol.OpMoveFragment] = s.moveFragmentOperation

	// Import
	for code, f := range s.operations {
		operations[code] = f
	}
}
