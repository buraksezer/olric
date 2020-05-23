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
	"github.com/julienschmidt/httprouter"
)

type errorResponse struct {
	Message string
}

func (db *Olric) httpErrorResponse(w http.ResponseWriter, err error) {
	e := errorResponse{
		Message: err.Error(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err == ErrKeyNotFound {
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
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, nilTimeout, 0, true)
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
	wr, err := db.prepareWriteop(protocol.OpPutIf, dmap, key, value, nilTimeout, int16(flags), true)
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
	rawtimeout := r.Header.Get("X-Olric-PutEx-Timeout")
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
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, timeout, 0, true)
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
	wr, err := db.prepareWriteop(protocol.OpPut, dmap, key, value, timeout, int16(flags), true)
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
	// TODO: Content-type should be read from Serializer implementation
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(value)
	if err != nil {
		db.log.V(6).Printf("[ERROR] Failed to write to ResponseWriter: %v", err)
	}
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
