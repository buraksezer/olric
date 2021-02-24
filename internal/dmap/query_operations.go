package dmap

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/query"
	"github.com/vmihailenco/msgpack"
)

func (s *Service) localQueryOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	q, err := query.FromByte(req.Value())
	if err != nil {
		errorResponse(w, err)
		return
	}

	dm, err := s.getDMap(req.DMap())
	if err == errFragmentNotFound {
		// TODO: This may be wrong
		w.SetStatus(protocol.StatusOK)
		return
	}
	if err != nil {
		errorResponse(w, err)
		return
	}

	partID := req.Extra().(protocol.LocalQueryExtra).PartID
	result, err := dm.runLocalQuery(partID, q)
	if err != nil {
		errorResponse(w, err)
		return
	}
	value, err := msgpack.Marshal(&result)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (s *Service) queryOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
		return
	}
	q, err := query.FromByte(req.Value())
	if err != nil {
		errorResponse(w, err)
		return
	}
	c, err := dm.Query(q)
	if err != nil {
		errorResponse(w, err)
		return
	}
	defer c.Close()

	partID := req.Extra().(protocol.QueryExtra).PartID
	if partID >= s.config.PartitionCount {
		errorResponse(w, ErrEndOfQuery)
		return
	}
	responses, err := c.runQueryOnOwners(partID)
	if err != nil {
		errorResponse(w, err)
		return
	}

	data := make(QueryResponse)
	for _, response := range responses {
		data[response.Key()] = response.Value()
	}

	value, err := msgpack.Marshal(data)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}
