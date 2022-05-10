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

package process

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/buraksezer/olric/pkg/flog"
	"golang.org/x/sync/errgroup"
)

type IProcess interface {
	Name() string
	Logger() *flog.Logger
	Start() error
	Shutdown(ctx context.Context) error
}

// Process represents a new olric-sequence-server instance.
type Process struct {
	log     *flog.Logger
	process IProcess
	errGr   errgroup.Group
}

func New(p IProcess) (*Process, error) {
	return &Process{
		process: p,
		log:     p.Logger(),
	}, nil
}

func (p *Process) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	p.log.V(1).Printf("[INFO] Signal catched: %s", ch.String())

	// Awaits for shutdown
	p.errGr.Go(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := p.process.Shutdown(ctx); err != nil {
			p.log.V(1).Printf("[ERROR] Failed to Shutdown %s: %v", p.process.Name(), err)
			return err
		}

		return nil
	})

	// This is not a goroutine leak. The process will quit.
	go func() {
		p.log.V(1).Printf("[INFO] Awaiting for background tasks")
		p.log.V(1).Printf("[INFO] Press CTRL+C or send SIGTERM/SIGINT to quit immediately")

		forceQuitCh := make(chan os.Signal, 1)
		signal.Notify(forceQuitCh, syscall.SIGTERM, syscall.SIGINT)
		ch := <-forceQuitCh

		p.log.V(1).Printf("[INFO] Signal caught: %s", ch.String())
		p.log.V(1).Printf("[INFO] Quits with exit code 1")
		os.Exit(1)
	}()
}

// Start starts a new olric-sequence-server instance and blocks until the server is closed.
func (p *Process) Start() error {
	p.log.V(1).Printf("[INFO] pid: %d has been started", os.Getpid())

	// Wait for SIGTERM or SIGINT
	go p.waitForInterrupt()

	p.errGr.Go(func() error {
		return p.process.Start()
	})

	return p.errGr.Wait()
}

// Shutdown stops background servers and leaves the cluster.
func (p *Process) Shutdown(ctx context.Context) error {
	return p.process.Shutdown(ctx)
}
