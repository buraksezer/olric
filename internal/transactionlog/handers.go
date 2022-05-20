package transactionlog

import (
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (t *TransactionLog) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pingCmd, err := protocol.ParsePingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if pingCmd.Message != "" {
		conn.WriteString(pingCmd.Message)
		return
	}
	conn.WriteString(olric.DefaultPingResponse)
}
