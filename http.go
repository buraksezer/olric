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
	"net/http"
	"sync/atomic"

	"github.com/buraksezer/olric/config"
	server "github.com/buraksezer/olric/internal/http"
	"github.com/buraksezer/olric/internal/http/middlewares/is_operable"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/julienschmidt/httprouter"
)

func (db *Olric) registerHTTPEndpoints() http.Handler {
	router := httprouter.New()
	// DMap API
	//
	// Put
	router.POST("/api/v1/dmap/put/:dmap/:key", db.dmapPutHTTPHandler)
	router.POST("/api/v1/dmap/putif/:dmap/:key", db.dmapPutIfHTTPHandler)
	router.POST("/api/v1/dmap/putex/:dmap/:key", db.dmapPutExHTTPHandler)
	router.POST("/api/v1/dmap/putifex/:dmap/:key", db.dmapPutIfExHTTPHandler)
	//
	// Expire
	router.PUT("/api/v1/dmap/expire/:dmap/:key", db.dmapExpireHTTPHandler)
	//
	// Get
	router.GET("/api/v1/dmap/get/:dmap/:key", db.dmapGetHTTPHandler)
	router.GET("/api/v1/dmap/get-entry/:dmap/:key", db.dmapGetEntryHTTPHandler)
	//
	// Delete
	router.DELETE("/api/v1/dmap/delete/:dmap/:key", db.dmapDeleteHTTPHandler)
	//
	// Destroy
	router.DELETE("/api/v1/dmap/destroy/:dmap", db.dmapDestroyHTTPHandler)
	//
	// Atomic operations
	router.PUT("/api/v1/dmap/incr/:dmap/:key/:delta", db.dmapIncrHTTPHandler)
	router.PUT("/api/v1/dmap/decr/:dmap/:key/:delta", db.dmapDecrHTTPHandler)
	router.PUT("/api/v1/dmap/getput/:dmap/:key", db.dmapGetPutHTTPHandler)
	//
	// Locking
	router.POST("/api/v1/dmap/lock-with-timeout/:dmap/:key", db.dmapLockWithTimeoutHTTPHandler)
	router.POST("/api/v1/dmap/lock/:dmap/:key", db.dmapLockHTTPHandler)
	router.PUT("/api/v1/dmap/unlock/:dmap/:key", db.dmapUnlockHTTPHandler)
	//
	// Query
	router.POST("/api/v1/dmap/query/:dmap/:partID", db.dmapQueryHTTPHandler)

	// System
	router.GET("/api/v1/system/stats", db.systemStatsHTTPHandler)
	router.GET("/api/v1/system/ping/:addr", db.systemPingHTTPHandler)

	// Check aliveness firstly, we don't want to accept connections until the HTTP server works without any problem.	w
	router.GET("/api/v1/system/aliveness", db.systemAlivenessHandler)
	return router
}

func (db *Olric) initializeHTTPIntegration(cfg *config.Http, flogger *flog.Logger) {
	defer atomic.AddInt32(&requiredCheckpoints, 1)
	router := server.NewRouter(
		db.registerHTTPEndpoints(),
		is_operable.New(db.isOperable))
	db.http = server.New(cfg, flogger, router)
}

func (db *Olric) systemStatsHTTPHandler(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	data, err := db.serializer.Marshal(db.stats())
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

func (db *Olric) systemPingHTTPHandler(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	addr := ps.ByName("addr")
	data, err := db.serializer.Marshal(db.Ping(addr))
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

func (db *Olric) systemAlivenessHandler(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusNoContent)
}
