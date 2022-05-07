package transaction

import "github.com/cespare/xxhash/v2"

const (
	ReadCommandKind = iota + 1
	MutateCommandKind
)

type Tx interface {
	Commit(readVersion, commitVersion uint32, commands []Command) error
}

type Command interface {
	HKey() uint64
	Key() string
	Kind() int
}

type GetCommand struct {
	hkey uint64
	key  string
	kind int
}

func (g *GetCommand) HKey() uint64 {
	return g.hkey
}

func (g *GetCommand) Kind() int {
	return g.kind
}

func NewReadCommand(key string) *GetCommand {
	return &GetCommand{
		hkey: xxhash.Sum64String(key),
		key:  key,
		kind: ReadCommandKind,
	}
}

func (g *GetCommand) Key() string {
	return g.key
}

type MutateCommand struct {
	hkey uint64
	key  string
	kind int
}

func (m *MutateCommand) HKey() uint64 {
	return m.hkey
}

func NewMutateCommand(key string) *MutateCommand {
	return &MutateCommand{
		hkey: xxhash.Sum64String(key),
		key:  key,
		kind: MutateCommandKind,
	}
}

func (m *MutateCommand) Key() string {
	return m.key
}

func (m *MutateCommand) Kind() int {
	return m.kind
}

var (
	_ Command = (*GetCommand)(nil)
	_ Command = (*MutateCommand)(nil)
)
