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
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
)

func (h *httpTransport) handleExGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	value, err := h.db.get(name, key)
	if err == ErrKeyNotFound {
		h.returnErr(w, err, http.StatusNotFound)
		return
	}
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	_, err = io.Copy(w, bytes.NewReader(value))
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleExPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	owner, hkey, err := h.db.locateKey(name, key)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	if !hostCmp(owner, h.db.this) {
		// Redirect this to the its owner.
		target := url.URL{
			Scheme: h.scheme,
			Host:   owner.String(),
			Path:   path.Join("/put/", name, printHKey(hkey)),
		}
		rtimeout := r.URL.Query().Get("t")
		if rtimeout != "" {
			q := target.Query()
			q.Set("t", rtimeout)
			target.RawQuery = q.Encode()
		}
		_, err = h.doRequest(http.MethodPost, target, r.Body)
		if err != nil {
			h.returnErr(w, err, http.StatusInternalServerError)
		}
		return
	}

	value, err := ioutil.ReadAll(r.Body)
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

func (h *httpTransport) handleExDelete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	if err := h.db.deleteKey(name, key); err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleExLockWithTimeout(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")

	rtimeout := r.URL.Query().Get("t")
	timeout, err := time.ParseDuration(rtimeout)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	err = h.db.lockWithTimeout(name, key, timeout)
	if err == ErrKeyNotFound {
		h.returnErr(w, err, http.StatusNotFound)
		return
	}
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleExUnlock(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")

	err := h.db.unlock(name, key)
	if err == ErrNoSuchLock {
		h.returnErr(w, err, http.StatusNotFound)
		return
	}
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleExDestroy(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	if err := h.db.destroyDMap(name); err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) exIncrDecr(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	key := ps.ByName("key")
	sdelta := ps.ByName("delta")
	delta, err := strconv.Atoi(sdelta)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	var value int
	if strings.HasPrefix(r.URL.Path, "/ex/incr") {
		value, err = h.db.atomicIncrDecr(name, key, "incr", delta)
	} else {
		value, err = h.db.atomicIncrDecr(name, key, "decr", delta)
	}
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	data, err := h.db.serializer.Marshal(value)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(w, bytes.NewReader(data))
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *httpTransport) handleExIncr(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	h.exIncrDecr(w, r, ps)
}

func (h *httpTransport) handleExDecr(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	h.exIncrDecr(w, r, ps)
}

func (h *httpTransport) handleExGetPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	name := ps.ByName("name")
	key := ps.ByName("key")
	oldval, err := h.db.getPut(name, key, value)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	if oldval != nil {
		_, err = io.Copy(w, bytes.NewReader(oldval))
		if err != nil {
			h.returnErr(w, err, http.StatusInternalServerError)
			return
		}
	}
}
