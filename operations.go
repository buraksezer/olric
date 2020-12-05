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

package olric

import "github.com/buraksezer/olric/internal/protocol"

func (db *Olric) registerOperations() {
	// Operations on DMap data structure
	//
	// DMap.Put
	db.operations[protocol.OpPut] = db.exPutOperation
	db.operations[protocol.OpPutEx] = db.exPutOperation
	db.operations[protocol.OpPutReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutExReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutIf] = db.exPutOperation
	db.operations[protocol.OpPutIfEx] = db.exPutOperation
	db.operations[protocol.OpPutIfReplica] = db.putReplicaOperation
	db.operations[protocol.OpPutIfExReplica] = db.putReplicaOperation

	// DMap.Get
	db.operations[protocol.OpGet] = db.exGetOperation
	db.operations[protocol.OpGetPrev] = db.getPrevOperation
	db.operations[protocol.OpGetBackup] = db.getBackupOperation

	// DMap.Delete
	db.operations[protocol.OpDelete] = db.exDeleteOperation
	db.operations[protocol.OpDeleteBackup] = db.deleteBackupOperation
	db.operations[protocol.OpDeletePrev] = db.deletePrevOperation

	// DMap.Lock
	db.operations[protocol.OpLockWithTimeout] = db.exLockWithTimeoutOperation
	db.operations[protocol.OpLock] = db.exLockOperation

	// DMap.Unlock
	db.operations[protocol.OpUnlock] = db.exUnlockOperation

	// DMap.Destroy
	db.operations[protocol.OpDestroy] = db.exDestroyOperation
	db.operations[protocol.OpDestroyDMap] = db.destroyDMapOperation

	// DMap.Atomic
	db.operations[protocol.OpIncr] = db.exIncrDecrOperation
	db.operations[protocol.OpDecr] = db.exIncrDecrOperation
	db.operations[protocol.OpGetPut] = db.exGetPutOperation

	// DMap.Pipeline
	db.operations[protocol.OpPipeline] = db.pipelineOperation

	// DMap.Expire
	db.operations[protocol.OpExpire] = db.exExpireOperation
	db.operations[protocol.OpExpireReplica] = db.expireReplicaOperation

	// DMap.Query (distributed query)
	db.operations[protocol.OpLocalQuery] = db.localQueryOperation
	db.operations[protocol.OpQuery] = db.exQueryOperation

	// System Messages
	//
	// Internal
	db.operations[protocol.OpUpdateRouting] = db.updateRoutingOperation
	db.operations[protocol.OpMoveDMap] = db.moveDMapOperation
	db.operations[protocol.OpLengthOfPart] = db.keyCountOnPartOperation

	// Aliveness
	db.operations[protocol.OpPing] = db.pingOperation

	// Node Stats
	db.operations[protocol.OpStats] = db.statsOperation

	// Operations on DTopic data structure
	//
	// DTopic.Publish
	db.operations[protocol.OpPublishDTopicMessage] = db.publishDTopicMessageOperation
	db.operations[protocol.OpDTopicPublish] = db.exDTopicPublishOperation

	// DTopic.Destroy
	db.operations[protocol.OpDestroyDTopic] = db.destroyDTopicOperation
	db.operations[protocol.OpDTopicDestroy] = db.exDTopicDestroyOperation

	// DTopic.AddListener
	db.operations[protocol.OpDTopicAddListener] = db.exDTopicAddListenerOperation

	// DTopic.RemoveListener
	db.operations[protocol.OpDTopicRemoveListener] = db.exDTopicRemoveListenerOperation

	// Operations on message streams
	//
	// Bidirectional communication channel for clients and cluster members.
	db.operations[protocol.OpCreateStream] = db.createStreamOperation
}
