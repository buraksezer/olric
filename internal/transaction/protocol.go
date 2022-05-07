package transaction

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

type TransactionCommands struct {
	Resolve string
}

var TransactionCmd = &TransactionCommands{
	Resolve: "transaction.resolve",
}

type TransactionResolve struct{}

func NewTransactionResolve() *TransactionResolve {
	return &TransactionResolve{}
}

func (c *TransactionResolve) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, TransactionCmd.Resolve)
	return redis.NewIntCmd(ctx, args...)
}

func ParseTransactionResolve(cmd redcon.Command) (*TransactionResolve, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewTransactionResolve()
	return c, nil
}
