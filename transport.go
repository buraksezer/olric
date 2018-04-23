// Copyright 2018 Burak Sezer
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

package olricdb

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
)

type httpTransport struct {
	startCh chan struct{}
	scheme  string
	ctx     context.Context
	db      *OlricDB
	logger  *log.Logger
	config  *Config
	client  *http.Client
	server  *http.Server
	router  *httprouter.Router
}

// newHTTPTransport implements Transport interface with HTTP and it also supports HTTP/2.
func newHTTPTransport(ctx context.Context, cfg *Config) *httpTransport {
	if cfg.Client == nil {
		cfg.Client = &http.Client{}
	}
	if cfg.Server == nil {
		cfg.Server = &http.Server{}
	}
	cfg.Server.Addr = cfg.Name

	scheme := "https"
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		scheme = "http"
	}
	h := &httpTransport{
		scheme:  scheme,
		startCh: make(chan struct{}),
		ctx:     ctx,
		config:  cfg,
		logger:  cfg.Logger,
		client:  cfg.Client,
		server:  cfg.Server,
		router:  httprouter.New(),
	}
	h.router.HEAD("/aliveness", h.handleAliveness)
	h.router.DELETE("/destroy-dmap/:name", h.handleDestroyDmap)
	h.router.POST("/move-dmap", h.handleMoveDmap)
	h.router.POST("/update-routing", h.handleUpdateRouting)
	h.router.GET("/is-part-empty/:partID", h.handleIsPartEmpty)
	h.router.GET("/is-backup-empty/:partID", h.handleIsBackupEmpty)

	h.router.POST("/put/:name/:hkey", h.handlePut)
	h.router.GET("/get/:name/:hkey", h.handleGet)
	h.router.DELETE("/delete/:name/:hkey", h.handleDelete)
	h.router.GET("/get-prev/:name/:hkey", h.handleGetPrev)
	h.router.DELETE("/delete-prev/:name/:hkey", h.handleDeletePrev)

	h.router.GET("/find-lock/:name/:key", h.handleFindLock)
	h.router.GET("/lock/:name/:key", h.handleLock)
	h.router.GET("/lock-prev/:name/:key", h.handleLockPrev)
	h.router.GET("/unlock/:name/:key", h.handleUnlock)
	h.router.GET("/unlock-prev/:name/:key", h.handleUnlockPrev)

	h.router.POST("/backup/put/:name/:hkey", h.handlePutBackup)
	h.router.GET("/backup/get/:name/:hkey", h.handleGetBackup)
	h.router.DELETE("/backup/delete/:name/:hkey", h.handleDeleteBackup)
	h.router.POST("/backup/move-dmap", h.handleMoveBackupDmap)

	// Endpoints for external access.
	h.router.GET("/ex/get/:name/:key", h.handleExGet)
	h.router.POST("/ex/put/:name/:key", h.handleExPut)
	h.router.DELETE("/ex/delete/:name/:key", h.handleExDelete)
	h.router.GET("/ex/lock-with-timeout/:name/:key", h.handleExLockWithTimeout)
	h.router.GET("/ex/unlock/:name/:key", h.handleExUnlock)
	h.router.GET("/ex/destroy/:name", h.handleExDestroy)
	return h
}

func (h *httpTransport) returnErr(w http.ResponseWriter, err interface{}, statusCode int) {
	var desc string
	switch err := err.(type) {
	case error:
		desc = err.(error).Error()
	case string:
		desc = err
	}
	w.WriteHeader(statusCode)
	_, err = fmt.Fprintf(w, desc)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to return error: %s", err)
	}
}

func (h *httpTransport) doRequest(method string, target url.URL, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, target.String(), body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(h.ctx)
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			h.logger.Printf("[ERROR] response body could not be closed: %v", err)
		}
	}()

	// HEAD request doesn't have body.
	if method == http.MethodHead {
		if resp.StatusCode == http.StatusOK {
			return nil, nil
		}
		return nil, fmt.Errorf("HEAD request failed with status code %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		return data, nil
	}

	msg := string(data)
	switch {
	case msg == ErrKeyNotFound.Error():
		return nil, ErrKeyNotFound
	case msg == ErrNoSuchLock.Error():
		return nil, ErrNoSuchLock
	case msg == errPartNotEmpty.Error():
		return nil, errPartNotEmpty
	case msg == errHostNotFound.Error():
		return nil, errHostNotFound
	case msg == ErrOperationTimeout.Error():
		return nil, ErrOperationTimeout
	}

	return nil, fmt.Errorf("unknown response received: %s", string(data))
}

func (h *httpTransport) updateRouting(member host, data []byte) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   "/update-routing",
	}
	body := bytes.NewReader(data)
	_, err := h.doRequest(http.MethodPost, target, body)
	return err
}

func (h *httpTransport) start() error {
	select {
	case <-h.startCh:
		return nil
	default:
	}
	defer close(h.startCh)
	h.server.Handler = h.router
	var err error
	if h.scheme == "https" {
		err = h.server.ListenAndServeTLS(h.config.CertFile, h.config.KeyFile)
	} else {
		err = h.server.ListenAndServe()
	}
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}

func (h *httpTransport) handleUpdateRouting(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	rt := make(routing)
	err := gob.NewDecoder(r.Body).Decode(&rt)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	for partID, data := range rt {
		// Set partition(primary copies) owners
		part := h.db.partitions[partID]
		part.Lock()
		part.owners = data.Owners
		part.Unlock()

		// Set backup owners
		bpart := h.db.backups[partID]
		bpart.Lock()
		bpart.owners = data.Backups
		bpart.Unlock()
	}

	// Bootstrapped by the coordinator.
	h.db.bcancel()
	h.db.wg.Add(1)
	go func() {
		defer h.db.wg.Done()
		h.db.fsck()
	}()
}

func (h *httpTransport) handleIsPartEmpty(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	raw := ps.ByName("partID")
	partID, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	part := h.db.partitions[partID]
	part.RLock()
	defer part.RUnlock()
	if len(part.m) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	h.returnErr(w, errPartNotEmpty, http.StatusBadRequest)
}

func (h *httpTransport) isPartEmpty(partID uint64, owner host) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   owner.String(),
		Path:   path.Join("/is-part-empty", strconv.FormatUint(partID, 10)),
	}
	_, err := h.doRequest(http.MethodGet, target, nil)
	return err
}

// alivenessHandler is a dummy endpoint which just returns HTTP 204 to caller. It can be used to check server aliveness.
func (h *httpTransport) handleAliveness(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}

func (h *httpTransport) checkAliveness(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if port == "0" {
		// Port number is 0. Passing aliveness check.
		return nil
	}

	target := url.URL{
		Host:   addr,
		Scheme: h.scheme,
		Path:   "/aliveness",
	}

	for i := 0; i < 10; i++ {
		_, err = h.doRequest(http.MethodHead, target, nil)
		if err != nil {
			<-time.After(100 * time.Millisecond)
			continue
		}
		break
	}
	return err
}
