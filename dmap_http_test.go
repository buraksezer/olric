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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
)

func TestHTTP_DMapGetKeyNotFound(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	router := httprouter.New()
	router.Handle(http.MethodGet, "/api/v1/dmap/get/:dmap/:key", db.dmapGetHTTPHandler)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dmap/get/mydmap/mykey", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	resp := rec.Body.Bytes()
	if rec.Code != http.StatusNotFound {
		t.Fatalf("Expected HTTP status code 404. Got: %d", rec.Code)
	}

	value, err := db.unmarshalValue(resp)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	errResp := value.(errorResponse)
	if errResp.Message != "key not found" {
		t.Fatalf("Expected key not found. Got: %s", errResp.Message)
	}
}

func TestHTTP_DMapGet(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodGet, "/api/v1/dmap/get/:dmap/:key", db.dmapGetHTTPHandler)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dmap/get/mydmap/mykey", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}
	resp := rec.Body.Bytes()
	value, err := db.unmarshalValue(resp)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != "myvalue" {
		t.Fatalf("Expected myvalue. Got: %v", value)
	}
}

func TestHTTP_DMapPut(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	body, err := db.serializer.Marshal("myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPost, "/api/v1/dmap/put/:dmap/:key", db.dmapPutHTTPHandler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dmap/put/mydmap/mykey", bytes.NewBuffer(body))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	value, err := dm.Get("mykey")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != "myvalue" {
		t.Fatalf("Expected myvalue. Got: %v", value)
	}
}

func TestHTTP_DMapPutIf(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	body, err := db.serializer.Marshal("myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPost, "/api/v1/dmap/putif/:dmap/:key", db.dmapPutIfHTTPHandler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dmap/putif/mydmap/mykey", bytes.NewBuffer(body))
	req.Header.Add("X-Olric-PutIf-Flags", strconv.FormatInt(int64(IfNotFound), 10))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	value, err := dm.Get("mykey")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != "myvalue" {
		t.Fatalf("Expected myvalue. Got: %v", value)
	}
}

func TestHTTP_DMapPutEx(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	body, err := db.serializer.Marshal("myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPost, "/api/v1/dmap/putex/:dmap/:key", db.dmapPutExHTTPHandler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dmap/putex/mydmap/mykey", bytes.NewBuffer(body))
	req.Header.Add("X-Olric-Timeout", "1")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 204. Got: %d", rec.Code)
	}

	<-time.After(10 * time.Millisecond)
	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestHTTP_DMapPutIfEx(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	body, err := db.serializer.Marshal("myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPost, "/api/v1/dmap/putifex/:dmap/:key", db.dmapPutIfExHTTPHandler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dmap/putifex/mydmap/mykey", bytes.NewBuffer(body))
	req.Header.Add("X-Olric-PutEx-Timeout", "1000") // 1 second
	req.Header.Add("X-Olric-PutIf-Flags", strconv.FormatInt(int64(IfNotFound), 10))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 204. Got: %d", rec.Code)
	}

	_, err = dm.Get("mykey")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Wait some time for expire
	<-time.After(1100 * time.Millisecond)
	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestHTTP_DMapExpire(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPost, "/api/v1/dmap/expire/:dmap/:key", db.dmapExpireHTTPHandler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dmap/expire/mydmap/mykey", nil)
	req.Header.Add("X-Olric-Timeout", "1") // 1ms

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 204. Got: %d", rec.Code)
	}

	<-time.After(10 * time.Millisecond)

	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestHTTP_DMapDelete(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodDelete, "/api/v1/dmap/delete/:dmap/:key", db.dmapDeleteHTTPHandler)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/dmap/delete/mydmap/mykey", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 204. Got: %d", rec.Code)
	}

	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestHTTP_DMapDestroy(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodDelete, "/api/v1/dmap/destroy/:dmap", db.dmapDestroyHTTPHandler)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/dmap/destroy/mydmap", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("Expected HTTP status code 204. Got: %d", rec.Code)
	}

	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestHTTP_DMapIncr(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	router := httprouter.New()
	router.Handle(http.MethodPut, "/api/v1/dmap/incr/:dmap/:key/:delta", db.dmapIncrHTTPHandler)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/dmap/incr/mydmap/mykey/10", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}

	value, err := db.unmarshalValue(rec.Body.Bytes())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != 10 {
		t.Fatalf("Expected 10. Got: %v", value)
	}
}

func TestHTTP_DMapDecr(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", 10)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPut, "/api/v1/dmap/decr/:dmap/:key/:delta", db.dmapDecrHTTPHandler)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/dmap/decr/mydmap/mykey/10", nil)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}

	value, err := db.unmarshalValue(rec.Body.Bytes())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != 0 {
		t.Fatalf("Expected 0. Got: %v", value)
	}
}

func TestHTTP_DMapGetPut(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	body, err := db.serializer.Marshal("updated-myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	router := httprouter.New()
	router.Handle(http.MethodPut, "/api/v1/dmap/getput/:dmap/:key", db.dmapGetPutHTTPHandler)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/dmap/getput/mydmap/mykey", bytes.NewBuffer(body))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected HTTP status code 200. Got: %d", rec.Code)
	}

	value, err := db.unmarshalValue(rec.Body.Bytes())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if value != "myvalue" {
		t.Fatalf("Expected myvalue. Got: %v", value)
	}
}
