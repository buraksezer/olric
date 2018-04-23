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
	"encoding/gob"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/julienschmidt/httprouter"
)

func (h *httpTransport) put(member host, hkey uint64, name string, value interface{}, timeout time.Duration) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/put/", name, printHKey(hkey)),
	}
	if timeout != nilTimeout {
		q := target.Query()
		q.Set("t", timeout.String())
		target.RawQuery = q.Encode()
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&value)
	if err != nil {
		return err
	}
	body := bytes.NewReader(buf.Bytes())
	_, err = h.doRequest(http.MethodPost, target, body)
	return err
}

func (h *httpTransport) handlePut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	owner, err := h.db.locateHKey(hkey)
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
	var value interface{}
	err = gob.NewDecoder(r.Body).Decode(&value)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	var timeout = nilTimeout
	rtimeout := r.URL.Query().Get("t")
	if rtimeout != "" {
		timeout, err = time.ParseDuration(rtimeout)
		if err != nil {
			h.returnErr(w, err, http.StatusInternalServerError)
			return
		}
	}
	err = h.db.putKeyVal(hkey, name, value, timeout)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) get(member host, hkey uint64, name string) (interface{}, error) {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/get/", name, printHKey(hkey)),
	}
	data, err := h.doRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	body := bytes.NewReader(data)
	var value interface{}
	if err := gob.NewDecoder(body).Decode(&value); err != nil {
		return nil, err
	}

	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

func (h *httpTransport) handleGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	owner, err := h.db.locateHKey(hkey)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	if !hostCmp(owner, h.db.this) {
		// Fail early.
		h.returnErr(w,
			fmt.Sprintf("this is %s. given hkey belongs to %s", h.db.this, owner),
			http.StatusConflict,
		)
		return
	}

	statusCode := http.StatusInternalServerError
	value, err := h.db.getKeyVal(hkey, name)
	if err == ErrKeyNotFound {
		statusCode = http.StatusNotFound
	}
	if err != nil {
		h.returnErr(w, err, statusCode)
		return
	}
	if value == nil {
		value = struct{}{}
	}
	registerValueType(value)
	err = gob.NewEncoder(w).Encode(&value)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleGetPrev(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	part := h.db.getPartition(hkey)
	part.RLock()
	dm, ok := part.m[name]
	if !ok {
		part.RUnlock()
		h.returnErr(w, ErrKeyNotFound, http.StatusNotFound)
		return
	}
	part.RUnlock()

	dm.RLock()
	vdata, ok := dm.d[hkey]
	if !ok {
		dm.RUnlock()
		h.returnErr(w, ErrKeyNotFound, http.StatusNotFound)
		return
	}
	dm.RUnlock()

	if isKeyExpired(vdata.TTL) {
		h.returnErr(w, ErrKeyNotFound, http.StatusNotFound)
		return
	}

	registerValueType(&vdata.Value)
	err = gob.NewEncoder(w).Encode(&vdata.Value)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) delete(member host, hkey uint64, name string) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/delete/", name, printHKey(hkey)),
	}
	_, err := h.doRequest(http.MethodDelete, target, nil)
	return err
}

func (h *httpTransport) handleDelete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	owner, err := h.db.locateHKey(hkey)
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
	err = h.db.deleteKeyVal(hkey, name)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
	}
}

func (h *httpTransport) handleDeletePrev(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	dm := h.db.getDMap(name, hkey)
	dm.Lock()
	defer dm.Unlock()
	delete(dm.d, hkey)

	if len(dm.d) == 0 {
		h.db.wg.Add(1)
		go h.db.deleteStaleDMap(name, hkey, false)
	}
}

func (h *httpTransport) deletePrev(owner host, hkey uint64, name string) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   owner.String(),
		Path:   path.Join("/delete-prev", name, printHKey(hkey)),
	}
	_, err := h.doRequest(http.MethodDelete, target, nil)
	if err != nil {
		return fmt.Errorf("Failed to call delete-prev on %s: %v", owner, err)
	}
	return err
}

func (h *httpTransport) handleMoveDmap(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	dbox := &dmapbox{}
	err := gob.NewDecoder(r.Body).Decode(dbox)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	part := h.db.partitions[dbox.PartID]
	part.RLock()
	if len(part.owners) == 0 {
		part.RUnlock()
		panic("partition owners list cannot be empty")
	}
	owner := part.owners[len(part.owners)-1]
	part.RUnlock()
	if !hostCmp(owner, h.db.this) {
		// Fail early.
		h.returnErr(w,
			fmt.Sprintf("this is %s. given key belongs to %s", h.db.this, owner),
			http.StatusConflict,
		)
		return
	}
	h.db.mergeDMaps(part, dbox)
}

func (h *httpTransport) moveDmap(data []byte, owner host) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   owner.String(),
		Path:   "/move-dmap",
	}
	body := bytes.NewReader(data)
	_, err := h.doRequest(http.MethodPost, target, body)
	return err
}

func (h *httpTransport) handleDestroyDmap(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	for partID := uint64(0); partID < h.db.config.PartitionCount; partID++ {
		// Delete primary copies
		part := h.db.partitions[partID]
		part.Lock()
		delete(part.m, name)
		part.Unlock()

		// Delete from Backups
		if h.db.config.BackupCount != 0 {
			bpart := h.db.backups[partID]
			bpart.Lock()
			delete(bpart.m, name)
			bpart.Unlock()
		}
	}
}

func (h *httpTransport) destroyDmap(name, addr string) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   addr,
		Path:   path.Join("/destroy-dmap", name),
	}
	_, err := h.doRequest(http.MethodDelete, target, nil)
	return err
}
