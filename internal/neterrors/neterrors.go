package neterrors

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"unsafe"

	"github.com/buraksezer/olric/internal/protocol"
)

// https://github.com/cosmos/cosmos-sdk/blob/master/types/errors/errors.go

const RootCodespace = "root"

var (
	mtx          sync.Mutex
	registryOnce sync.Once
	registry     map[string]map[protocol.StatusCode]error
)

// NetError defines a custom error type.
type NetError struct {
	message    string
	statusCode protocol.StatusCode
}

func New(codespace string, code protocol.StatusCode, message string) *NetError {
	e := &NetError{
		statusCode: code,
		message:    message,
	}
	registryOnce.Do(func() {
		registry = make(map[string]map[protocol.StatusCode]error)
	})
	mtx.Lock()
	defer mtx.Unlock()
	_, ok := registry[codespace]
	if !ok {
		registry[codespace] = make(map[protocol.StatusCode]error)
	}
	_, ok = registry[codespace][code]
	if ok {
		panic(fmt.Sprintf("an error has already been registered with StatusCode: %d", code))
	}
	registry[codespace][code] = e
	return e
}

func (e *NetError) Error() string {
	return e.message
}

func (e *NetError) Bytes() []byte {
	return *(*[]byte)(unsafe.Pointer(&e.message))
}

func (e *NetError) StatusCode() protocol.StatusCode {
	return e.statusCode
}

// Wrap extends this error with an additional information.
func Wrap(err error, message string) error {
	return errors.Wrap(err, message)
}

func GetByCode(codespace string, code protocol.StatusCode) error {
	if code == protocol.StatusOK {
		return nil
	}
	tmp, ok := registry[codespace]
	if !ok {
		return fmt.Errorf("no codespace found: %s", codespace)
	}
	err, ok := tmp[code]
	if !ok {
		return fmt.Errorf("no error found with StatusCode: %d", code)
	}
	return err
}
