package olric

import "github.com/buraksezer/olric/internal/protocol"

// Represents a hook handler
//
// Before operation hooks use the return values, however After operation hooks completely ignore these values
type HookHandler func(key string, value interface{}) (modifiedKey string, modifiedValue interface{}, err error)

const (
	BeforePutHook   = int8(protocol.OpPut) * -1
	AfterPutHook    = int8(protocol.OpPut)
	AfterExpireHook = protocol.OpExpire
)

// Registers a handler
func (dm *DMap) RegisterHook(hook int8, handler HookHandler) {
	if harr, ok := dm.hooks[hook]; ok {
		harr = append(harr, handler)
		dm.hooks[hook] = harr // update
	} else {
		dm.hooks[hook] = []HookHandler{handler}
	}
}
