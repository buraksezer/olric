package olricdb

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
)

func (h *httpTransport) handleIsBackupEmpty(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	raw := ps.ByName("partID")
	partID, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	part := h.db.backups[partID]
	part.RLock()
	defer part.RUnlock()

	if len(part.m) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	h.returnErr(w, errPartNotEmpty, http.StatusBadRequest)
}

func (h *httpTransport) isBackupEmpty(partID uint64, owner host) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   owner.String(),
		Path:   path.Join("/is-backup-empty", strconv.FormatUint(partID, 10)),
	}
	_, err := h.doRequest(http.MethodGet, target, nil)
	return err
}

func (h *httpTransport) putBackup(member host, name string, hkey uint64, value interface{}, timeout time.Duration) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/backup/put/", name, printHKey(hkey)),
	}
	if timeout != nilTimeout {
		q := target.Query()
		q.Set("t", timeout.String())
		target.RawQuery = q.Encode()
	}
	buf, err := h.db.serializer.Marshal(value)
	if err != nil {
		return err
	}
	body := bytes.NewReader(buf)
	_, err = h.doRequest(http.MethodPost, target, body)
	return err
}

func (h *httpTransport) handlePutBackup(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	// TODO: We may need to check backup ownership
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}

	var value interface{}
	err = h.db.serializer.Unmarshal(data, &value)
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

	dmp := h.db.getBackupDmap(name, hkey)
	dmp.Lock()
	defer dmp.Unlock()

	var ttl int64
	if timeout != nilTimeout {
		ttl = getTTL(timeout)
	}
	dmp.d[hkey] = vdata{
		Value: value,
		TTL:   ttl,
	}
}

func (h *httpTransport) handleDeleteBackup(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	// TODO: We may need to check backup ownership
	dm := h.db.getBackupDmap(name, hkey)
	dm.Lock()
	defer dm.Unlock()
	delete(dm.d, hkey)

	if len(dm.d) == 0 {
		h.db.wg.Add(1)
		go h.db.deleteStaleDMap(name, hkey, true)
	}
}

func (h *httpTransport) deleteBackup(member host, hkey uint64, name string) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   member.String(),
		Path:   path.Join("/backup/delete", name, printHKey(hkey)),
	}
	_, err := h.doRequest(http.MethodDelete, target, nil)
	return err
}

func (h *httpTransport) handleGetBackup(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	hkey, err := readHKey(ps)
	if err != nil {
		h.returnErr(w, err, http.StatusInternalServerError)
		return
	}
	// TODO: We may need to check backup ownership
	dm := h.db.getBackupDmap(name, hkey)
	dm.RLock()
	defer dm.RUnlock()
	vdata, ok := dm.d[hkey]
	if !ok {
		h.returnErr(w, ErrKeyNotFound, http.StatusNotFound)
		return
	}

	if isKeyExpired(vdata.TTL) {
		h.returnErr(w, ErrKeyNotFound, http.StatusNotFound)
		return
	}

	data, err := h.db.serializer.Marshal(vdata.Value)
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

func (h *httpTransport) handleMoveBackupDmap(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to read request body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	dbox := &dmapbox{}
	err = h.db.serializer.Unmarshal(data, dbox)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to unmarshal dmap for backup: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	bpart := h.db.backups[dbox.PartID]
	bpart.RLock()
	if len(bpart.owners) == 0 {
		bpart.RUnlock()
		panic("backup owners list cannot be empty")
	}
	bpart.RUnlock()
	// TODO: Check backup ownership here
	h.db.mergeDMaps(bpart, dbox)
}

func (h *httpTransport) moveBackupDmap(data []byte, owner host) error {
	target := url.URL{
		Scheme: h.scheme,
		Host:   owner.String(),
		Path:   "/backup/move-dmap",
	}
	body := bytes.NewReader(data)
	_, err := h.doRequest(http.MethodPost, target, body)
	return err
}
