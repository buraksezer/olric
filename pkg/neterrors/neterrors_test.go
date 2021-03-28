package neterrors

import (
	"bytes"
	"errors"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestNetErrors_Wrap(t *testing.T) {
	errOne := Wrap(ErrUnknownOperation, "buggy client")

	if !errors.Is(errOne, ErrUnknownOperation) {
		t.Fatalf("Error is not ErrUnknownOperation")
	}
	if Unwrap(errOne) != ErrUnknownOperation {
		t.Fatalf("Unwrapped error is not ErrUnknownOperation")
	}

	errTwo := Wrap(ErrInvalidArgument, errOne)
	if !errors.Is(errTwo, ErrInvalidArgument) {
		t.Fatalf("Error is not ErrInvalidArgument")
	}
	if Unwrap(errTwo) != ErrInvalidArgument {
		t.Fatalf("Unwrapped error is not ErrInvalidArgument")
	}

	expectedErrorMessage := "invalid argument: unknown operation: buggy client"
	if errTwo.Error() != expectedErrorMessage {
		t.Fatalf("Expected %v. Got: %v", expectedErrorMessage, errTwo)
	}
}

func TestNetError_ErrorResponse_One_Wrap(t *testing.T) {
	errOne := Wrap(ErrUnknownOperation, "buggy client")
	w := protocol.NewDMapMessage(protocol.OpPut)
	w.SetBuffer(bytes.NewBuffer(nil))

	ErrorResponse(w, errOne)
	if string(w.Value()) != errOne.Error() {
		t.Fatalf("Expected %v. Got: %v", errOne.Error(), string(w.Value()))
	}

	if w.Status() != protocol.StatusErrUnknownOperation {
		t.Fatalf("Expected %v. Got: %v", protocol.StatusErrUnknownOperation, w.Status())
	}
}

func TestNetError_ErrorResponse_Two_Wrap(t *testing.T) {
	errOne := Wrap(ErrUnknownOperation, "buggy client")
	errTwo := Wrap(ErrInvalidArgument, errOne)
	if errTwo == nil {
		t.Fatalf("Expected an error. Got nil")
	}

	w := protocol.NewDMapMessage(protocol.OpPut)
	w.SetBuffer(bytes.NewBuffer(nil))
	ErrorResponse(w, errTwo)
	if string(w.Value()) != errTwo.Error() {
		t.Fatalf("Expected %v. Got: %v", errTwo.Error(), string(w.Value()))
	}

	if w.Status() != protocol.StatusErrInvalidArgument {
		t.Fatalf("Expected %v. Got: %v", protocol.StatusErrInvalidArgument, w.Status())
	}
}

func TestNetError_ErrorResponse_NetError(t *testing.T) {
	w := protocol.NewDMapMessage(protocol.OpPut)
	w.SetBuffer(bytes.NewBuffer(nil))
	ErrorResponse(w, ErrInvalidArgument)
	if string(w.Value()) != ErrInvalidArgument.Error() {
		t.Fatalf("Expected %v. Got: %v", ErrInvalidArgument.Error(), string(w.Value()))
	}

	if w.Status() != protocol.StatusErrInvalidArgument {
		t.Fatalf("Expected %v. Got: %v", protocol.StatusErrInvalidArgument, w.Status())
	}
}

func TestNetError_ErrorResponse_String(t *testing.T) {
	w := protocol.NewDMapMessage(protocol.OpPut)
	w.SetBuffer(bytes.NewBuffer(nil))
	message := "something went wrong"
	ErrorResponse(w, message)
	if string(w.Value()) != message {
		t.Fatalf("Expected %v. Got: %v", message, string(w.Value()))
	}

	if w.Status() != protocol.StatusErrInternalFailure {
		t.Fatalf("Expected %v. Got: %v", protocol.StatusErrInternalFailure, w.Status())
	}
}

func TestNetError_ErrorResponse_error(t *testing.T) {
	var err = errors.New("foobar error")
	w := protocol.NewDMapMessage(protocol.OpPut)
	w.SetBuffer(bytes.NewBuffer(nil))
	ErrorResponse(w, err)
	if string(w.Value()) != err.Error() {
		t.Fatalf("Expected %v. Got: %v", err.Error(), string(w.Value()))
	}

	if w.Status() != protocol.StatusErrInternalFailure {
		t.Fatalf("Expected %v. Got: %v", protocol.StatusErrInternalFailure, w.Status())
	}
}

func TestNetErrors_GetByCode(t *testing.T) {
	err := GetByCode(protocol.StatusErrInternalFailure)
	if err != ErrInternalFailure {
		t.Fatalf("Expected: %v. Got: %v", ErrInternalFailure, err)
	}

	message := "no error found with StatusCode: 244"
	err = GetByCode(244)
	if err == nil {
		t.Fatalf("Expected an error. Got nil.")
	}
	if err.Error() != message {
		t.Fatalf("Expected: %s. Got: %v", message, err)
	}
}
