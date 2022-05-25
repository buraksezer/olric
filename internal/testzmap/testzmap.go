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

package testzmap

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/resolver"
	resolverConfig "github.com/buraksezer/olric/internal/resolver/config"
	"github.com/buraksezer/olric/internal/sequencer"
	sequencerConfig "github.com/buraksezer/olric/internal/sequencer/config"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

type TestZMap struct {
	storageCluster  *testcluster.TestCluster
	sequencer       *sequencer.Sequencer
	sequencerConfig *sequencerConfig.Config
	resolver        *resolver.Resolver
	resolverConfig  *resolverConfig.Config
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

func makeSequencerConfig() (*sequencerConfig.Config, error) {
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}

	tmpdir, err := ioutil.TempDir("", "olric-zmap-sequencer")
	if err != nil {
		return nil, err
	}

	return &sequencerConfig.Config{
		OlricSequencer: sequencerConfig.OlricSequencer{
			BindAddr:        "localhost",
			BindPort:        port,
			KeepAlivePeriod: "300s",
			DataDir:         tmpdir,
		},
		Logging: sequencerConfig.Logging{
			Verbosity: 6,
			Level:     "DEBUG",
			Output:    "stderr",
		},
	}, nil
}

func makeResolverConfig() (*resolverConfig.Config, error) {
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}

	tmpdir, err := ioutil.TempDir("", "olric-zmap-resolver")
	if err != nil {
		return nil, err
	}

	return &resolverConfig.Config{
		OlricResolver: resolverConfig.OlricResolver{
			BindAddr:        "localhost",
			BindPort:        port,
			KeepAlivePeriod: "300s",
			DataDir:         tmpdir,
		},
		Logging: resolverConfig.Logging{
			Verbosity: 6,
			Level:     "DEBUG",
			Output:    "stderr",
		},
	}, nil
}

func New(t *testing.T, constructor func(e *environment.Environment) (service.Service, error)) *TestZMap {
	lg := log.New(os.Stderr, "", log.LstdFlags)

	sc, err := makeSequencerConfig()
	require.NoError(t, err)

	sq, err := sequencer.New(sc, lg)
	require.NoError(t, err)

	rsc, err := makeResolverConfig()
	require.NoError(t, err)
	rs, err := resolver.New(rsc, lg)

	ctx, cancel := context.WithCancel(context.Background())
	storageCluster := testcluster.New(constructor)

	tz := &TestZMap{
		storageCluster:  storageCluster,
		sequencer:       sq,
		sequencerConfig: sc,
		resolver:        rs,
		resolverConfig:  rsc,
		ctx:             ctx,
		cancel:          cancel,
	}

	go func() {
		if err = tz.sequencer.Start(); err != nil {
			panic(fmt.Sprintf("failed to run sequencer: %s", err))
		}
	}()

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		rc := redis.NewClient(&redis.Options{Addr: tz.SequencerAddr()})
		defer func() {
			require.NoError(t, rc.Close())
		}()
		cmd := rc.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	go func() {
		if err = tz.resolver.Start(); err != nil {
			panic(fmt.Sprintf("failed to run resolver: %s", err))
		}
	}()

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		rc := redis.NewClient(&redis.Options{Addr: tz.ResolverAddr()})
		defer func() {
			require.NoError(t, rc.Close())
		}()
		cmd := rc.Ping(ctx)
		return cmd.Err()
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		tz.shutdown()
	})

	return tz
}

func (tz *TestZMap) SequencerAddr() string {
	return net.JoinHostPort(
		tz.sequencerConfig.OlricSequencer.BindAddr,
		strconv.Itoa(tz.sequencerConfig.OlricSequencer.BindPort),
	)
}

func (tz *TestZMap) ResolverAddr() string {
	return net.JoinHostPort(
		tz.resolverConfig.OlricResolver.BindAddr,
		strconv.Itoa(tz.resolverConfig.OlricResolver.BindPort),
	)
}

func (tz *TestZMap) AddStorageNode(e *environment.Environment) service.Service {
	if e == nil {
		c := testutil.NewConfig()
		c.Cluster.Sequencer.Addr = tz.SequencerAddr()
		e = testcluster.NewEnvironment(c)
	} else {
		olricConfig := e.Get("config").(*config.Config)
		olricConfig.Cluster.Sequencer.Addr = tz.SequencerAddr()
		e.Set("config", olricConfig)
	}

	return tz.storageCluster.AddMember(e)
}

func (tz *TestZMap) shutdown() {
	tz.storageCluster.Shutdown()

	err := tz.sequencer.Shutdown(context.Background())
	if err != nil {
		panic(fmt.Sprintf("failed to shutdown sequencer: %v", err))
	}

	err = tz.resolver.Shutdown(context.Background())
	if err != nil {
		panic(fmt.Sprintf("failed to shutdown resolver: %v", err))
	}

	dirs := []string{
		tz.sequencerConfig.OlricSequencer.DataDir,
		tz.resolverConfig.OlricResolver.DataDir,
	}
	for _, dir := range dirs {
		err = os.RemoveAll(dir)
		if err != nil {
			panic(fmt.Sprintf("failed to remove directory: %s: %v", dir, err))
		}
	}
}
