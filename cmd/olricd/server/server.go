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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/http2"

	"github.com/buraksezer/olricdb"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/logutils"
)

// Olricd represents a new Olricd instance.
type Olricd struct {
	logger *log.Logger
	config *olricdb.Config
	db     *olricdb.OlricDB
}

func newHTTPClient(c *Config) (*http.Client, error) {
	dialerTimeout, err := time.ParseDuration(c.HTTPClient.DialerTimeout)
	if err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(c.HTTPClient.Timeout)
	if err != nil {
		return nil, err
	}

	if c.Olricd.CertFile == "" || c.Olricd.KeyFile == "" {
		return &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					KeepAlive: 5 * time.Minute,
					Timeout:   dialerTimeout,
				}).DialContext,
			},
		}, nil
	}

	tc := &tls.Config{InsecureSkipVerify: c.HTTPClient.InsecureSkipVerify}
	dialTLS := func(network, addr string, cfg *tls.Config) (net.Conn, error) {
		d := &net.Dialer{Timeout: dialerTimeout}
		return tls.DialWithDialer(d, network, addr, cfg)
	}
	return &http.Client{
		Transport: &http2.Transport{
			DialTLS:         dialTLS,
			TLSClientConfig: tc,
		},
		Timeout: timeout,
	}, nil
}

// New creates a new Server instance
func New(c *Config) (*Olricd, error) {
	s := &Olricd{}
	var logDest io.Writer
	if c.Logging.Output == "stderr" {
		logDest = os.Stderr
	} else if c.Logging.Output == "stdout" {
		logDest = os.Stdout
	} else {
		logDest = os.Stderr
	}

	if c.Logging.Level == "" {
		c.Logging.Level = olricdb.DefaultLogLevel
	}

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.Logging.Level)),
		Writer:   logDest,
	}
	s.logger = log.New(logDest, "", log.LstdFlags)
	s.logger.SetOutput(filter)

	client, err := newHTTPClient(c)
	if err != nil {
		return nil, err
	}

	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var serializer olricdb.Serializer
	if c.Olricd.Serializer == "json" {
		serializer = olricdb.NewJsonSerializer()
	} else if c.Olricd.Serializer == "msgpack" {
		serializer = olricdb.NewMsgpackSerializer()
	} else if c.Olricd.Serializer == "gob" {
		serializer = olricdb.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", c.Olricd.Serializer)
	}

	mc, err := newMemberlistConf(c)
	if err != nil {
		return nil, err
	}
	s.config = &olricdb.Config{
		Name:             c.Olricd.Name,
		MemberlistConfig: mc,
		KeyFile:          c.Olricd.KeyFile,
		CertFile:         c.Olricd.CertFile,
		LogLevel:         c.Logging.Level,
		Peers:            c.Memberlist.Peers,
		PartitionCount:   c.Olricd.PartitionCount,
		BackupCount:      c.Olricd.BackupCount,
		BackupMode:       c.Olricd.BackupMode,
		LoadFactor:       c.Olricd.LoadFactor,
		Client:           client,
		Logger:           s.logger,
		Hasher:           hasher{},
		Serializer:       serializer,
	}
	return s, nil
}

func (s *Olricd) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	s.logger.Printf("[INFO] Signal catched: %s", ch.String())
	s.Shutdown()
}

// Start starts a new olricd server instance and blocks until the server is closed.
func (s *Olricd) Start() error {
	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	db, err := olricdb.New(s.config)
	if err != nil {
		return err
	}
	s.db = db
	s.logger.Printf("[INFO] olricd (pid: %d) has been started on %s", os.Getpid(), s.config.Name)
	return s.db.Start()
}

// Shutdown calls olricdb.Shutdown for graceful shutdown.
func (s *Olricd) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.db.Shutdown(ctx)
	if err != nil {
		s.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
	}
}

// olricd uses xxhash as hash function for performance.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}
