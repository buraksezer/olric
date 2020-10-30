// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package skiplist

// Iterator is a Skiplist Iterator that buffers upcoming results, so that it does
// not have to acquire the Skiplist's mtx on each Next call.
type Iterator struct {
	m *Skiplist
	// restartNode is the node to start refilling the buffer from.
	restartNode int

	limitNode int

	// i0 is the current Iterator position with respect to buf. A value of -1
	// means that the Iterator is at the start, end or both of the iteration.
	// i1 is the number of buffered entries.
	// Invariant: -1 <= i0 && i0 < i1 && i1 <= len(buf).
	i0, i1 int
	// buf buffers up to 32 key/value pairs.
	buf [32][2][]byte
}

// NewIterator implements DB.NewIterator, as documented in the leveldb/db package.
func (s *Skiplist) NewIterator() *Iterator {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	n, _ := s.findNode(nil, nil)
	for n != zeroNode && s.nodeData[n+fVal] == kvOffsetDeletedNode {
		n = s.nodeData[n+fNxt]
	}
	t := &Iterator{
		m:           s,
		restartNode: n,
	}
	t.fill()
	// The Iterator is positioned at the first node >= key. The Iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}

func (s *Skiplist) SubMap(fromKey, toKey []byte) *Iterator {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	n, _ := s.findNode(fromKey, nil)
	for n != zeroNode && s.nodeData[n+fVal] == kvOffsetDeletedNode {
		n = s.nodeData[n+fNxt]
	}

	l, _ := s.findNode(toKey, nil)
	t := &Iterator{
		m:           s,
		restartNode: n,
		limitNode:   l,
	}
	t.fill()
	// The Iterator is positioned at the first node >= key. The Iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}

// fill fills the Iterator's buffer with key/value pairs from the Skiplist.
//
// Precondition: t.m.mtx is locked for reading.
func (t *Iterator) fill() {
	i, n := 0, t.restartNode
	for i < len(t.buf) && n != zeroNode && n != t.limitNode {
		if t.m.nodeData[n+fVal] != kvOffsetDeletedNode {
			t.buf[i][fKey] = t.m.load(t.m.nodeData[n+fKey])
			t.buf[i][fVal] = t.m.load(t.m.nodeData[n+fVal])
			i++
		}
		n = t.m.nodeData[n+fNxt]
	}
	if i == 0 {
		// There were no non-deleted nodes on or after t.restartNode.
		// The Iterator is exhausted.
		t.i0 = -1
	} else {
		t.i0 = 0
	}
	t.i1 = i
	t.restartNode = n
}

// Next implements Iterator.Next, as documented in the leveldb/db package.
func (t *Iterator) Next() bool {
	t.i0++
	if t.i0 < t.i1 {
		return true
	}
	if t.restartNode == zeroNode || t.restartNode == t.limitNode {
		t.i0 = -1
		t.i1 = 0
		return false
	}
	t.m.mtx.RLock()
	defer t.m.mtx.RUnlock()

	t.fill()
	return true
}

// key implements Iterator.key, as documented in the leveldb/db package.
func (t *Iterator) Key() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fKey]
}

// value implements Iterator.value, as documented in the leveldb/db package.
func (t *Iterator) Value() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fVal]
}
