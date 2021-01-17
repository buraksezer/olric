package dset

import "github.com/buraksezer/olric/internal/dmap"

type Cursor struct {

}

type DSet struct {
	dm *dmap.DMap
}

func New() *DSet {
	return &DSet{}
}

func (ds *DSet) Add(key string) error {
	return nil
}

func (ds *DSet) Delete(key string) error {
	return nil
}

func (ds *DSet) Length() int {
	return 0
}

func (ds *DSet) Diff(s *DSet) (*Cursor, error) {
	return nil, nil
}

func (ds *DSet) Inter(s *DSet)(*Cursor, error) {
	return nil, nil
}

func (ds *DSet) Check(key string) (bool, error) {
	return false, nil
}