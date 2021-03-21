package neterrors

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/buraksezer/olric/internal/protocol"
)

var (
	mtx          sync.Mutex
	registryOnce sync.Once
	registry     map[protocol.StatusCode]error
)

// NetError defines a custom error type.
type NetError struct {
	message    string
	statusCode protocol.StatusCode
}

func New(message string, statusCode protocol.StatusCode) *NetError {
	e := &NetError{
		statusCode: statusCode,
		message:    message,
	}
	registryOnce.Do(func() {
		registry = make(map[protocol.StatusCode]error)
	})
	mtx.Lock()
	defer mtx.Unlock()
	_, ok := registry[statusCode]
	if ok {
		panic(fmt.Sprintf("an error has already been registered with StatusCode: %d", statusCode))
	}
	registry[statusCode] = e
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

func GetByCode(code protocol.StatusCode) error {
	if code == protocol.StatusOK {
		return nil
	}
	err, ok := registry[code]
	if !ok {
		return fmt.Errorf("no error found with StatusCode: %d", code)
	}
	return err
}
