package transaction

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (tr *Transaction) transactionResolveHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := ParseTransactionResolve(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
}
