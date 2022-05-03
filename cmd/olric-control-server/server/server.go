// Copyright 2018-2022 Burak Sezer
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

/*Package server provides a standalone server implementation for Olric*/
package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/buraksezer/olric/cmd/olric-control-server/config"
	"github.com/buraksezer/olric/internal/controller"
	"golang.org/x/sync/errgroup"
)

// OlricControlServer represents a new olric-control-server instance.
type OlricControlServer struct {
	log        *log.Logger
	config     *config.Config
	controller *controller.Controller
	errGr      errgroup.Group
}

// New creates a new olric-control-server instance
func New(c *config.Config, lg *log.Logger) (*OlricControlServer, error) {
	ctr, err := controller.New(c, lg)
	if err != nil {
		return nil, err
	}
	return &OlricControlServer{
		config:     c,
		log:        lg,
		controller: ctr,
	}, nil
}

func (s *OlricControlServer) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	s.log.Printf("[INFO] Signal catched: %s", ch.String())

	// Awaits for shutdown
	s.errGr.Go(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := s.controller.Shutdown(ctx); err != nil {
			s.log.Printf("[ERROR] Failed to shutdown olric-control-server: %v", err)
			return err
		}

		return nil
	})

	// This is not a goroutine leak. The process will quit.
	go func() {
		s.log.Printf("[INFO] Awaiting for background tasks")
		s.log.Printf("[INFO] Press CTRL+C or send SIGTERM/SIGINT to quit immediately")

		forceQuitCh := make(chan os.Signal, 1)
		signal.Notify(forceQuitCh, syscall.SIGTERM, syscall.SIGINT)
		ch := <-forceQuitCh

		s.log.Printf("[INFO] Signal caught: %s", ch.String())
		s.log.Printf("[INFO] Quits with exit code 1")
		os.Exit(1)
	}()
}

// Start starts a new olric-control-server instance and blocks until the server is closed.
func (s *OlricControlServer) Start() error {
	s.log.Printf("[INFO] pid: %d has been started", os.Getpid())

	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	s.errGr.Go(func() error {
		return s.controller.Start()
	})

	return s.errGr.Wait()
}

// Shutdown stops background servers and leaves the cluster.
func (s *OlricControlServer) Shutdown(ctx context.Context) error {
	return s.controller.Shutdown(ctx)
}
