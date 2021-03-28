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

// Operations
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
	OpDestroyDMapInternal
	OpMoveDMap // TODO: Rename to OpMoveFragment
	OpLengthOfPart
	OpPipeline
	OpPing
	OpStats
	OpExpire
	OpExpireReplica
	OpQuery
	OpLocalQuery
	OpPublishDTopicMessage
	OpDestroyDTopicInternal
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

// Status Codes
const (
	StatusOK                  = StatusCode(iota) + 1
	StatusErrInternalFailure  // 2
	StatusErrKeyNotFound      // 3
	StatusErrNoSuchLock       // 4
	StatusErrLockNotAcquired  // 5
	StatusErrWriteQuorum      // 6
	StatusErrReadQuorum       // 7
	StatusErrOperationTimeout // 8
	StatusErrKeyFound         // 9
	StatusErrClusterQuorum    // 10
	StatusErrUnknownOperation // 11
	StatusErrEndOfQuery       // 12
	StatusErrServerGone       // 13
	StatusErrInvalidArgument  // 14
	StatusErrKeyTooLarge      // 15
	StatusErrNotImplemented   // 16
)
