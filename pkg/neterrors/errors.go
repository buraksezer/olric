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

package neterrors

import "github.com/buraksezer/olric/internal/protocol"

var (
	ErrInvalidArgument  = New(protocol.StatusErrInvalidArgument, "invalid argument")
	ErrUnknownOperation = New(protocol.StatusErrUnknownOperation, "unknown operation")
	ErrInternalFailure  = New(protocol.StatusErrInternalFailure, "internal failure")
	ErrNotImplemented   = New(protocol.StatusErrNotImplemented, "not implemented")
	ErrOperationTimeout = New(protocol.StatusErrOperationTimeout, "operation timeout")
)
