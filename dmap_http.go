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

import (
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/query"
	"github.com/julienschmidt/httprouter"
	"github.com/vmihailenco/msgpack"
)

// TODO: Check this: checkOperationStatus

type errorResponse struct {
	Message string
}

func (db *Olric) httpErrorResponse(w http.ResponseWriter, err error) {
	e := errorResponse{
		Message: err.Error(),
	}
	w.Header().Set("Content-Type", db.config.Http.ContentType)
	if err == ErrKeyNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else if err == ErrLockNotAcquired {
		// Deadline exceeded
		w.WriteHeader(http.StatusRequestTimeout)
	} else if err == ErrNoSuchLock {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	data, err := db.serializer.Marshal(e)
	if err != nil {
		db.log.V(6).Printf("[DEBUG] Failed to serialize error response: %v", err)
		return
	}
	_, _ = w.Write(data)
}

func (db *Olric) dmapPutHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, nilTimeout, 0)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	err = db.put(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapPutIfHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	var flags int64
	var err error
	rawflags := r.Header.Get("X-Olric-PutIf-Flags")
	if rawflags != "" {
		flags, err = strconv.ParseInt(rawflags, 10, 16)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
	}
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	wr, err := db.prepareWriteop(protocol.OpPutIf, dmap, key, value, nilTimeout, int16(flags))
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	err = db.put(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapPutExHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	timeout := nilTimeout
	rawtimeout := r.Header.Get("X-Olric-Timeout")
	if rawtimeout != "" {
		ttl, err := strconv.ParseInt(rawtimeout, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		timeout = time.Duration(ttl) * time.Millisecond
	}

	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, timeout, 0)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	err = db.put(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapPutIfExHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	timeout := nilTimeout
	rawtimeout := r.Header.Get("X-Olric-PutEx-Timeout")
	if rawtimeout != "" {
		ttl, err := strconv.ParseInt(rawtimeout, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		timeout = time.Duration(ttl) * time.Millisecond
	}

	var flags int64
	var err error
	rawflags := r.Header.Get("X-Olric-PutIf-Flags")
	if rawflags != "" {
		flags, err = strconv.ParseInt(rawflags, 10, 16)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
	}

	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, timeout, int16(flags))
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	err = db.put(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapGetHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	value, err := db.get(dmap, key)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.Header().Set("Content-Type", db.config.Http.ContentType)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(value.Value)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapGetEntryHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	raw, err := db.get(dmap, key)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	val, err := db.unmarshalValue(raw.Value)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	entry := Entry{
		Key:       raw.Key,
		Value:     val,
		TTL:       raw.TTL,
		Timestamp: raw.Timestamp,
	}
	w.Header().Set("Content-Type", db.config.Http.ContentType)
	w.WriteHeader(http.StatusOK)
	data, err := db.serializer.Marshal(entry)
	_, err = w.Write(data)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapExpireHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	timeout := nilTimeout
	rawtimeout := r.Header.Get("X-Olric-Timeout")
	if rawtimeout != "" {
		ttl, err := strconv.ParseInt(rawtimeout, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		timeout = time.Duration(ttl) * time.Millisecond
	}

	wr := &writeop{
		dmap:      dmap,
		key:       key,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
	}

	err := db.expire(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapDeleteHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	err := db.deleteKey(dmap, key)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapDestroyHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	err := db.destroyDMap(dmap)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapIncrDecrHTTP(opcode protocol.OpCode, w http.ResponseWriter, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	rawdelta := ps.ByName("delta")

	delta, err := strconv.ParseInt(rawdelta, 10, 64)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	wr, err := db.prepareWriteop(opcode, dmap, key, nil, nilTimeout, 0)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	value, err := db.atomicIncrDecr(opcode, wr, int(delta))
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	data, err := db.serializer.Marshal(value)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	_, err = w.Write(data)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapIncrHTTPHandler(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	db.dmapIncrDecrHTTP(protocol.OpIncr, w, ps)
}

func (db *Olric) dmapDecrHTTPHandler(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	db.dmapIncrDecrHTTP(protocol.OpDecr, w, ps)
}

func (db *Olric) dmapGetPutHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	wr, err := db.prepareWriteop(protocol.OpGetPut, dmap, key, value, nilTimeout, 0)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	data, err := db.getPut(wr)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapLockWithTimeoutHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	var err error

	timeout := nilTimeout
	rawtimeout := r.Header.Get("X-Olric-Timeout")
	if rawtimeout != "" {
		ttl, err := strconv.ParseInt(rawtimeout, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		timeout = time.Duration(ttl) * time.Millisecond
	}

	deadline := nilTimeout
	rawdeadline := r.Header.Get("X-Olric-Lock-Deadline")
	if rawdeadline != "" {
		rd, err := strconv.ParseInt(rawdeadline, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		deadline = time.Duration(rd) * time.Millisecond
	}

	ctx, err := db.lockKey(protocol.OpPutIfEx, dmap, key, timeout, deadline)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write(ctx.token)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapLockHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")
	var err error

	deadline := nilTimeout
	rawdeadline := r.Header.Get("X-Olric-Lock-Deadline")
	if rawdeadline != "" {
		rd, err := strconv.ParseInt(rawdeadline, 10, 64)
		if err != nil {
			db.httpErrorResponse(w, err)
			return
		}
		deadline = time.Duration(rd) * time.Millisecond
	}

	ctx, err := db.lockKey(protocol.OpPutIf, dmap, key, nilTimeout, deadline)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write(ctx.token)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}

func (db *Olric) dmapUnlockHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	key := ps.ByName("key")

	token, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	err = db.unlock(dmap, key, token)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (db *Olric) dmapQueryHTTPHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dmap := ps.ByName("dmap")
	rawPartID := ps.ByName("partID")

	partID, err := strconv.ParseInt(rawPartID, 10, 64)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	q, err := query.FromByte(data)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	c := &Cursor{
		db:    db,
		name:  dmap,
		query: q,
	}
	responses, err := c.runQueryOnOwners(uint64(partID))
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}

	result := make(QueryResponse)
	for _, response := range responses {
		result[response.Key] = response.Value
	}

	serialized, err := msgpack.Marshal(result)
	if err != nil {
		db.httpErrorResponse(w, err)
		return
	}
	_, err = w.Write(serialized)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
}
