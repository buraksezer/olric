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
	"errors"
	"strconv"
	"strings"

	"github.com/buraksezer/olric/internal/util"
	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
)

const DefaultRedisStringDMap = "0"

// TODO: RedisStringSet should support KEEPTTL and GET options

type RedisStringSet struct {
	Key   string
	Value []byte
	EX    float64
	PX    int64
	EXAT  float64
	PXAT  int64
	NX    bool
	XX    bool
}

func NewRedisStringSet(key string, value []byte) *RedisStringSet {
	return &RedisStringSet{
		Key:   key,
		Value: value,
	}
}

func (rs *RedisStringSet) SetEX(ex float64) *RedisStringSet {
	rs.EX = ex
	return rs
}

func (rs *RedisStringSet) SetPX(px int64) *RedisStringSet {
	rs.PX = px
	return rs
}

func (rs *RedisStringSet) SetEXAT(exat float64) *RedisStringSet {
	rs.EXAT = exat
	return rs
}

func (rs *RedisStringSet) SetPXAT(pxat int64) *RedisStringSet {
	rs.PXAT = pxat
	return rs
}

func (rs *RedisStringSet) SetNX() *RedisStringSet {
	rs.NX = true
	return rs
}

func (rs *RedisStringSet) SetXX() *RedisStringSet {
	rs.XX = true
	return rs
}

func (rs *RedisStringSet) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, RedisString.Set)
	args = append(args, rs.Key)
	args = append(args, rs.Value)

	if rs.EX != 0 {
		args = append(args, "EX")
		args = append(args, rs.EX)
	}

	if rs.PX != 0 {
		args = append(args, "PX")
		args = append(args, rs.PX)
	}

	if rs.EXAT != 0 {
		args = append(args, "EXAT")
		args = append(args, rs.EXAT)
	}

	if rs.PXAT != 0 {
		args = append(args, "PXAT")
		args = append(args, rs.PXAT)
	}

	if rs.NX {
		args = append(args, "NX")
	}

	if rs.XX {
		args = append(args, "XX")
	}

	return redis.NewStatusCmd(ctx, args...)
}

func ParseRedisStringSetCommand(cmd redcon.Command) (*RedisStringSet, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewRedisStringSet(
		util.BytesToString(cmd.Args[1]), // Key
		cmd.Args[2],                     // Value
	)

	args := cmd.Args[3:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "NX":
			p.SetNX()
			args = args[1:]
			continue
		case "XX":
			p.SetXX()
			args = args[1:]
			continue
		case "PX":
			px, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPX(px)
			args = args[2:]
			continue
		case "EX":
			ex, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEX(ex)
			args = args[2:]
			continue
		case "EXAT":
			exat, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEXAT(exat)
			args = args[2:]
			continue
		case "PXAT":
			pxat, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPXAT(pxat)
			args = args[2:]
			continue
		default:
			return nil, errors.New("syntax error")
		}
	}

	return p, nil
}
