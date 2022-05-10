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
	"log"
	"os"
	"testing"

	"github.com/buraksezer/olric/pkg/flog"
	"github.com/stretchr/testify/require"
)

type mockProcess struct {
	logger  *flog.Logger
	started chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

func (m *mockProcess) Name() string {
	return "mock-process"
}

func (m *mockProcess) Logger() *flog.Logger {
	return m.logger
}

func (m *mockProcess) Start() error {
	close(m.started)
	<-m.ctx.Done()
	return nil
}

func (m *mockProcess) Shutdown(ctx context.Context) error {
	m.cancel()
	return nil
}

var _ IProcess = (*mockProcess)(nil)

func TestProcess_Start_Shutdown(t *testing.T) {
	l := log.New(os.Stderr, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	p := &mockProcess{
		logger:  flog.New(l),
		started: make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
	proc, err := New(p)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- proc.Start()
	}()

	<-p.started

	require.NoError(t, proc.Shutdown(context.Background()))
	require.NoError(t, <-errCh)
}
