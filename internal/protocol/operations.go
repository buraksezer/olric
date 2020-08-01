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

package protocol

type OpCode uint8

// ops
const (
	OpPut = OpCode(iota) + 1
	OpPutEx
	OpPutIf
	OpPutIfEx
	OpGet
	OpDelete
	OpDestroy
	OpLock
	OpLockWithTimeout
	OpUnlock
	OpIncr
	OpDecr
	OpGetPut
	OpUpdateRouting
	OpPutReplica
	OpPutIfReplica
	OpPutExReplica
	OpPutIfExReplica
	OpDeletePrev
	OpGetPrev
	OpGetBackup
	OpDeleteBackup
	OpDestroyDMap
	OpMoveDMap
	OpLengthOfPart
	OpPipeline
	OpPing
	OpStats
	OpExpire
	OpExpireReplica
	OpQuery
	OpLocalQuery
	OpPublishDTopicMessage
	OpDestroyDTopic
	OpDTopicPublish
	OpDTopicAddListener
	OpDTopicRemoveListener
	OpDTopicDestroy
	OpCreateStream
	OpStreamCreated
	OpStreamMessage
	OpStreamPing
	OpStreamPong
)

type StatusCode uint8

// status codes
const (
	StatusOK = StatusCode(iota) + 1
	StatusInternalServerError
	StatusErrKeyNotFound
	StatusErrNoSuchLock
	StatusErrLockNotAcquired
	StatusErrWriteQuorum
	StatusErrReadQuorum
	StatusErrOperationTimeout
	StatusErrKeyFound
	StatusErrClusterQuorum
	StatusErrUnknownOperation
	StatusErrEndOfQuery
	StatusErrServerGone
	StatusErrInvalidArgument
	StatusErrKeyTooLarge
	StatusErrNotImplemented
)
