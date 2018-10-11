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

/*Package client implements a Golang client to access an OlricDB cluster from outside. */
package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/buraksezer/olricdb"
)

var nilTimeout = 0 * time.Second

// Client represents a Golang client to access an OlricDB cluster from outside.
type Client struct {
	servers    []string
	client     *http.Client
	serializer olricdb.Serializer
	ctx        context.Context
	cancel     context.CancelFunc
}

// New returns a new Client object. The second parameter is http.Client. It can be nil.
// Server names have to be start with protocol scheme: http or https.
func New(servers []string, c *http.Client, s olricdb.Serializer) (*Client, error) {
	if c == nil {
		c = &http.Client{}
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("servers slice cannot be empty")
	}

	if s == nil {
		s = olricdb.NewGobSerializer()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		servers:    servers,
		client:     c,
		serializer: s,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Close cancels underlying context and cancels ongoing requests.
func (c *Client) Close() {
	c.cancel()
}

func (c *Client) doRequest(method string, target url.URL, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, target.String(), body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(c.ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("[ERROR] response body could not be closed: %v", err)
		}
	}()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		return data, nil
	}

	msg := string(data)
	switch {
	case msg == olricdb.ErrKeyNotFound.Error():
		return nil, olricdb.ErrKeyNotFound
	case msg == olricdb.ErrNoSuchLock.Error():
		return nil, olricdb.ErrNoSuchLock
	case msg == olricdb.ErrOperationTimeout.Error():
		return nil, olricdb.ErrOperationTimeout
	}
	return nil, fmt.Errorf("unknown response received: %s", string(data))
}

// pickHosts selects a host randomly for query.
// TODO: We may want to implement a different algorithm such as round-robin.
func (c *Client) pickHost() (string, string, error) {
	i := rand.Intn(len(c.servers))
	picked := c.servers[i]
	p, err := url.Parse(picked)
	if err != nil {
		return "", "", err
	}
	return p.Scheme, p.Host, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.
// It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.
func (c *Client) Get(name, key string) (interface{}, error) {
	scheme, server, err := c.pickHost()
	if err != nil {
		return nil, err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/get/", name, key),
	}
	raw, err := c.doRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	var value interface{}
	err = c.serializer.Unmarshal(raw, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Client) put(name, key string, value interface{}, timeout time.Duration) error {
	scheme, server, err := c.pickHost()
	if err != nil {
		return err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/put/", name, key),
	}
	if timeout != nilTimeout {
		q := target.Query()
		q.Set("t", timeout.String())
		target.RawQuery = q.Encode()
	}

	if value == nil {
		value = struct{}{}
	}
	data, err := c.serializer.Marshal(value)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)
	_, err = c.doRequest(http.MethodPost, target, body)
	return err
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (c *Client) Put(name, key string, value interface{}) error {
	return c.put(name, key, value, nilTimeout)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (c *Client) PutEx(name, key string, value interface{}, timeout time.Duration) error {
	return c.put(name, key, value, timeout)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (c *Client) Delete(name, key string) error {
	scheme, server, err := c.pickHost()
	if err != nil {
		return err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/delete/", name, key),
	}
	_, err = c.doRequest(http.MethodDelete, target, nil)
	return err
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this map. Please note that, before
// setting a lock for a key, you should set the key with Put method. Otherwise it returns olricdb.ErrKeyNotFound error.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until timeout.
// The timeout is determined by http.Client which can be configured via Config structure.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (c *Client) LockWithTimeout(name, key string, timeout time.Duration) error {
	scheme, server, err := c.pickHost()
	if err != nil {
		return err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/lock-with-timeout/", name, key),
	}
	q := target.Query()
	q.Set("t", timeout.String())
	target.RawQuery = q.Encode()
	_, err = c.doRequest(http.MethodGet, target, nil)
	return err
}

// Unlock releases an acquired lock for the given key. It returns olricdb.ErrNoSuchLock if there is no lock for the given key.
func (c *Client) Unlock(name, key string) error {
	scheme, server, err := c.pickHost()
	if err != nil {
		return err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/unlock/", name, key),
	}
	_, err = c.doRequest(http.MethodGet, target, nil)
	return err
}

// Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.
// So if you call Put/PutEx and Destroy methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.
func (c *Client) Destroy(name string) error {
	scheme, server, err := c.pickHost()
	if err != nil {
		return err
	}
	target := url.URL{
		Scheme: scheme,
		Host:   server,
		Path:   path.Join("/ex/destroy/", name),
	}
	_, err = c.doRequest(http.MethodGet, target, nil)
	return err
}
