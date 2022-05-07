package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
)

// TODO: Merge with internal/protocol/cluster.go

func errWrongNumber(args [][]byte) error {
	sb := strings.Builder{}
	for {
		arg := args[0]
		sb.Write(arg)
		args = args[1:]
		if len(args) == 0 {
			break
		}
		sb.WriteByte(0x20)
	}
	return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(sb.String()))
}

type ClusterCommands struct {
	CommitVersion string
	ReadVersion   string
}

var Cluster = &ClusterCommands{
	CommitVersion: "cluster.commitversion",
	ReadVersion:   "cluster.readversion",
}

type ClusterCommitVersion struct{}

func NewClusterCommitVersion() *ClusterCommitVersion {
	return &ClusterCommitVersion{}
}

func (c *ClusterCommitVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Cluster.CommitVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseClusterCommitVersion(cmd redcon.Command) (*ClusterCommitVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewClusterCommitVersion()
	return c, nil
}

type ClusterReadVersion struct{}

func NewClusterReadVersion() *ClusterReadVersion {
	return &ClusterReadVersion{}
}

func (c *ClusterReadVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Cluster.CommitVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseClusterReadVersion(cmd redcon.Command) (*ClusterReadVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewClusterReadVersion()
	return c, nil
}
