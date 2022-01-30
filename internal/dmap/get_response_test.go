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

package dmap

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/encoding"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func TestDMap_Get_GetResponse(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	t.Run("Scan", func(t *testing.T) {
		var value = 100
		err = dm.Put("mykey-scan", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-scan")
		require.NoError(t, err)

		scannedValue := new(int)
		err = gr.Scan(scannedValue)
		require.NoError(t, err)
		require.Equal(t, value, *scannedValue)
	})

	t.Run("Byte", func(t *testing.T) {
		var value = []byte("olric")
		err = dm.Put("mykey-byte", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-byte")
		require.NoError(t, err)

		scannedValue, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int", func(t *testing.T) {
		var value = 100
		err = dm.Put("mykey-Int", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int")
		require.NoError(t, err)

		scannedValue, err := gr.Int()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("String", func(t *testing.T) {
		var value = "olric"
		err = dm.Put("mykey-String", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-String")
		require.NoError(t, err)

		scannedValue, err := gr.String()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int8", func(t *testing.T) {
		var value int8 = 10
		err = dm.Put("mykey-Int8", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int8")
		require.NoError(t, err)

		scannedValue, err := gr.Int8()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int16", func(t *testing.T) {
		var value int16 = 10
		err = dm.Put("mykey-Int16", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int16")
		require.NoError(t, err)

		scannedValue, err := gr.Int16()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int32", func(t *testing.T) {
		var value int32 = 10
		err = dm.Put("mykey-Int32", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int32")
		require.NoError(t, err)

		scannedValue, err := gr.Int32()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int64", func(t *testing.T) {
		var value int64 = 10
		err = dm.Put("mykey-Int64", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int64")
		require.NoError(t, err)

		scannedValue, err := gr.Int64()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Int64", func(t *testing.T) {
		var value int64 = 10
		err = dm.Put("mykey-Int64", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Int64")
		require.NoError(t, err)

		scannedValue, err := gr.Int64()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Uint", func(t *testing.T) {
		var value uint = 10
		err = dm.Put("mykey-Uint", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Uint")
		require.NoError(t, err)

		scannedValue, err := gr.Uint()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Uint8", func(t *testing.T) {
		var value uint8 = 10
		err = dm.Put("mykey-Uint8", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Uint8")
		require.NoError(t, err)

		scannedValue, err := gr.Uint8()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Uint16", func(t *testing.T) {
		var value uint16 = 10
		err = dm.Put("mykey-Uint16", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Uint16")
		require.NoError(t, err)

		scannedValue, err := gr.Uint16()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Uint32", func(t *testing.T) {
		var value uint32 = 10
		err = dm.Put("mykey-Uint32", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Uint32")
		require.NoError(t, err)

		scannedValue, err := gr.Uint32()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Uint64", func(t *testing.T) {
		var value uint64 = 10
		err = dm.Put("mykey-Uint64", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Uint64")
		require.NoError(t, err)

		scannedValue, err := gr.Uint64()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Float32", func(t *testing.T) {
		var value float32 = 10.12
		err = dm.Put("mykey-Float32", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Float32")
		require.NoError(t, err)

		scannedValue, err := gr.Float32()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Float64", func(t *testing.T) {
		var value = 10.12
		err = dm.Put("mykey-Float64", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Float64")
		require.NoError(t, err)

		scannedValue, err := gr.Float64()
		require.NoError(t, err)
		require.Equal(t, value, scannedValue)
	})

	t.Run("Bool", func(t *testing.T) {
		err = dm.Put("mykey-Bool", true)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-Bool")
		require.NoError(t, err)

		scannedValue, err := gr.Bool()
		require.NoError(t, err)
		require.Equal(t, true, scannedValue)
	})

	t.Run("time.Time", func(t *testing.T) {
		var value = time.Now()
		err = dm.Put("mykey-time.Time", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-time.Time")
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		enc := encoding.New(buf)
		err = enc.Encode(value)
		require.NoError(t, err)

		expectedValue := new(time.Time)
		err = encoding.Scan(buf.Bytes(), expectedValue)
		require.NoError(t, err)

		scannedValue, err := gr.Time()
		require.NoError(t, err)
		require.Equal(t, *expectedValue, scannedValue)
	})

	t.Run("time.Duration", func(t *testing.T) {
		var value = time.Second
		err = dm.Put("mykey-time.Duration", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-time.Duration")
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		enc := encoding.New(buf)
		err = enc.Encode(value)
		require.NoError(t, err)

		expectedValue := new(time.Duration)
		err = encoding.Scan(buf.Bytes(), expectedValue)
		require.NoError(t, err)

		scannedValue, err := gr.Duration()
		require.NoError(t, err)
		require.Equal(t, *expectedValue, scannedValue)
	})

	t.Run("BinaryUnmarshaler", func(t *testing.T) {
		var value = &myType{
			Database: "olric",
		}
		err = dm.Put("mykey-BinaryUnmarshaler", value)
		require.NoError(t, err)

		gr, err := dm.Get("mykey-BinaryUnmarshaler")
		require.NoError(t, err)

		v := myType{}
		err = gr.Scan(&v)
		require.NoError(t, err)
		require.Equal(t, value, &v)
	})
}

type myType struct {
	Database string
}

func (mt *myType) MarshalBinary() ([]byte, error) {
	return json.Marshal(mt)
}

func (mt *myType) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &mt)
}
