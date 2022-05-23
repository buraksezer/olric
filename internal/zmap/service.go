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

package zmap

import (
	"context"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/pkg/flog"
)

type Service struct {
	log    *flog.Logger
	config *config.Config
	client *server.Client
	server *server.Server
	zmaps  map[string]*ZMap
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config: e.Get("config").(*config.Config),
		client: e.Get("client").(*server.Client),
		server: e.Get("server").(*server.Server),
		log:    e.Get("logger").(*flog.Logger),
		zmaps:  make(map[string]*ZMap),
		ctx:    ctx,
		cancel: cancel,
	}
	s.RegisterHandlers()
	return s, nil
}

func (s *Service) Start() error {
	return nil
}

func (s *Service) RegisterHandlers() {}

func (s *Service) Shutdown(ctx context.Context) error {
	return nil
}

var _ service.Service = (*Service)(nil)
