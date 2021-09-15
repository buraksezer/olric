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

package protocol

type OpCode uint8

// Operations
const (
	OpPut                   = OpCode(iota) + 1
	OpPutEx                 // 2
	OpPutIf                 // 3
	OpPutIfEx               // 4
	OpGet                   // 5
	OpDelete                // 6
	OpDestroy               // 7
	OpLock                  // 8
	OpLockWithTimeout       // 9
	OpUnlock                // 10
	OpIncr                  // 11
	OpDecr                  // 12
	OpGetPut                // 13
	OpUpdateRouting         // 14
	OpPutReplica            // 15
	OpPutIfReplica          // 16
	OpPutExReplica          // 17
	OpPutIfExReplica        // 18
	OpDeletePrev            // 19
	OpGetPrev               // 20
	OpGetReplica            // 21
	OpDeleteReplica         // 22
	OpDestroyDMapInternal   // 23
	OpMoveFragment          // 24
	OpLengthOfPart          // 25
	OpPipeline              // 26
	OpPing                  // 27
	OpStats                 // 28
	OpExpire                // 29
	OpExpireReplica         // 30
	OpQuery                 // 31
	OpLocalQuery            // 32
	OpPublishDTopicMessage  // 33
	OpDestroyDTopicInternal // 34
	OpDTopicPublish         // 35
	OpDTopicAddListener     // 36
	OpDTopicRemoveListener  // 37
	OpDTopicDestroy         // 38
	OpCreateStream          // 39
	OpStreamCreated         // 40
	OpStreamMessage         // 41
	OpStreamPing            // 42
	OpStreamPong            // 43
	OpLockLease             // 44
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
