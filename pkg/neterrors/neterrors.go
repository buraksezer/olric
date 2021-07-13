// Copyright 2018-2021 Burak Sezer
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

package neterrors

import (
	"errors"
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

func New(code protocol.StatusCode, message string) *NetError {
	e := &NetError{
		statusCode: code,
		message:    message,
	}
	registryOnce.Do(func() {
		registry = make(map[protocol.StatusCode]error)
	})
	mtx.Lock()
	defer mtx.Unlock()
	_, ok := registry[code]
	if ok {
		panic(fmt.Sprintf("an error has already been registered with StatusCode: %d", code))
	}
	registry[code] = e
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
func Wrap(err error, message interface{}) error {
	return fmt.Errorf("%w: %v", err, message)
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
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

func toByte(err interface{}) []byte {
	switch val := err.(type) {
	case string:
		return []byte(val)
	case error:
		return []byte(val.Error())
	default:
		return nil
	}
}

func ErrorResponse(w protocol.EncodeDecoder, msg interface{}) {
	netErr, ok := msg.(*NetError)
	if ok {
		w.SetValue(netErr.Bytes())
		w.SetStatus(netErr.StatusCode())
		return
	}

	err, ok := msg.(error)
	if !ok {
		w.SetValue(toByte(msg))
		w.SetStatus(protocol.StatusErrInternalFailure)
		return
	}

	var tmp error
	for {
		tmp = Unwrap(err)
		if tmp == nil {
			break
		}
		err = tmp
	}
	netErr, ok = err.(*NetError)
	if !ok {
		w.SetValue(toByte(msg))
		w.SetStatus(protocol.StatusErrInternalFailure)
		return
	}
	w.SetValue(toByte(msg))
	w.SetStatus(netErr.StatusCode())
}
