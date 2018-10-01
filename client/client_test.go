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

package client

import (
	"context"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olricdb"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newOlricDB() (*olricdb.OlricDB, string, chan struct{}, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, "", nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	cfg := &olricdb.Config{Name: addr}
	db, err := olricdb.New(cfg)
	if err != nil {
		return nil, "", nil, err
	}

	done := make(chan struct{})
	go func() {
		rerr := db.Start()
		if rerr != nil {
			log.Printf("[ERROR] Expected nil. Got %v", rerr)
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	return db, addr, done, nil
}

func TestClient_Get(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	dm := db.NewDMap(dname)
	key, value := "my-key", "my-value"
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := c.Get(dname, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_Put(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key, value := "my-key", "my-value"
	err = c.Put(dname, key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm := db.NewDMap(dname)
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_PutEx(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key, value := "my-key", "my-value"
	err = c.PutEx(dname, key, value, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	dm := db.NewDMap(dname)
	_, err = dm.Get(key)
	if err != olricdb.ErrKeyNotFound {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_Delete(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key, value := "my-key", "my-value"
	err = c.Put(dname, key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.Delete(dname, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.Get(dname, key)
	if err != olricdb.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestClient_LockWithTimeout(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key := "my-key"
	err = c.Put(dname, key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.LockWithTimeout(dname, key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm := db.NewDMap(dname)
	err = dm.Unlock(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_Unlock(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key := "my-key"
	err = c.Put(dname, key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.LockWithTimeout(dname, key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.Unlock(dname, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_Destroy(t *testing.T) {
	db, addr, done, err := newOlricDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	servers := []string{"http://" + addr}
	c, err := New(servers, nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dname := "mymap"
	key := "my-key"
	err = c.Put(dname, key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.Destroy(dname)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.Get(dname, key)
	if err != olricdb.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}
