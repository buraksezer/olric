// Copyright 2018-2022 Burak Sezer
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

import (
	"context"
	"strconv"

	"github.com/buraksezer/olric/internal/util"
	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
)

type TransactionLogCommands struct {
	Add string
	Get string
}

var TransactionLog = &TransactionLogCommands{
	Add: "transactionlog.add",
	Get: "transactionlog.get",
}

type TransactionLogAdd struct {
	CommitVersion uint32
	Data          []byte
}

func NewTransactionLogAdd(commitVersion uint32, data []byte) *TransactionLogAdd {
	return &TransactionLogAdd{
		CommitVersion: commitVersion,
		Data:          data,
	}
}

func (ts *TransactionLogAdd) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, TransactionLog.Add)
	args = append(args, ts.CommitVersion)
	args = append(args, ts.Data)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseTransactionLogAdd(cmd redcon.Command) (*TransactionLogAdd, error) {
	if len(cmd.Args) != 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	commitVersion, err := strconv.ParseUint(util.BytesToString(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	ts := NewTransactionLogAdd(uint32(commitVersion), cmd.Args[2])
	return ts, nil
}

type TransactionLogGet struct {
	ReadVersion uint32
}

func NewTransactionLogGet(readVersion uint32) *TransactionLogGet {
	return &TransactionLogGet{ReadVersion: readVersion}
}

func (ts *TransactionLogGet) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, TransactionLog.Get)
	args = append(args, ts.ReadVersion)
	return redis.NewStringCmd(ctx, args...)
}

func ParseTransactionLogGet(cmd redcon.Command) (*TransactionLogGet, error) {
	if len(cmd.Args) != 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	readVersion, err := strconv.ParseUint(util.BytesToString(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	ts := NewTransactionLogGet(uint32(readVersion))
	return ts, nil
}
