package testutil

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/hashicorp/memberlist"
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

func NewFlogger(c *config.Config) *flog.Logger {
	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	return flogger
}

func NewConfig() *config.Config {
	c := config.New("local")
	c.PartitionCount = 7
	mc := memberlist.DefaultLocalConfig()
	mc.BindAddr = "127.0.0.1"
	mc.BindPort = 0
	c.MemberlistConfig = mc

	port, err := getFreePort()
	if err != nil {
		panic(fmt.Sprintf("getFreePort returned an error: %v", err))
	}
	c.BindAddr = "127.0.0.1"
	c.BindPort = port
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	return c
}

func NewTransportServer(c *config.Config) *transport.Server {
	sc := &transport.ServerConfig{
		BindAddr:        c.BindAddr,
		BindPort:        c.BindPort,
		KeepAlivePeriod: time.Second,
		GracefulPeriod:  10 * time.Second,
	}
	flogger := NewFlogger(c)
	srv := transport.NewServer(sc, flogger)
	return srv
}

