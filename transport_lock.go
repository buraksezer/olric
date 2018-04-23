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
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/julienschmidt/httprouter"
)

func (h *httpTransport) handleLock(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	owner, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	if !hostCmp(owner, h.db.this) {
		// Fail early.
		h.returnErr(w,
			fmt.Sprintf("this is %s. given key belongs to %s", h.db.this, owner),
			http.StatusConflict,
		)
		return
	}

	rawtm := r.URL.Query().Get("timeout")
	timeout, err := time.ParseDuration(rawtm)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	err = h.db.lockKey(hkey, name, key, timeout)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
	}
}

func (h *httpTransport) handleFindLock(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	_, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	dm := h.db.getDMap(name, hkey)
	if dm.locker.check(key) {
		w.WriteHeader(http.StatusOK)
		return
	}
	dm.RLock()
	defer dm.RUnlock()
	_, ok := dm.d[hkey]
	if ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	h.returnErr(w, ErrNoSuchLock, http.StatusBadRequest)
}

func (h *httpTransport) lock(member host, name, key string, timeout time.Duration) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/lock/", name, key),
	}
	q := target.Query()
	q.Set("timeout", timeout.String())
	target.RawQuery = q.Encode()
	_, err := h.doRequest(http.MethodGet, target, nil)
	return err
}

func (h *httpTransport) unlock(member host, name, key string) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/unlock/", name, key),
	}
	_, err := h.doRequest(http.MethodGet, target, nil)
	return err
}

func (h *httpTransport) handleUnlock(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	owner, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	if !hostCmp(owner, h.db.this) {
		// Fail early.
		h.returnErr(w,
			fmt.Sprintf("this is %s. given key belongs to %s", h.db.this, owner),
			http.StatusConflict,
		)
		return
	}
	err = h.db.unlockKey(hkey, name, key)
	if err == ErrNoSuchLock {
		h.returnErr(w, err, http.StatusBadGateway)
		return
	}
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
	}
}

func (h *httpTransport) handleUnlockPrev(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	_, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	dmp := h.db.getDMap(name, hkey)
	err = dmp.locker.unlock(key)
	if err != nil {
		h.returnErr(w, err, http.StatusBadRequest)
		return
	}
}

func (h *httpTransport) handleLockPrev(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	_, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	rawtm := r.URL.Query().Get("timeout")
	timeout, err := time.ParseDuration(rawtm)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	dmp := h.db.getDMap(name, hkey)
	dmp.locker.lock(key)
	h.db.wg.Add(1)
	go h.db.waitLockForTimeout(dmp, key, timeout)
}
