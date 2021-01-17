package dset

import "github.com/buraksezer/olric/internal/protocol"

func (s *Service) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
}
