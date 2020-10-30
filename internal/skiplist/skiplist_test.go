// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package skiplist

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// count returns the number of entries in a DB.
func count(s *Skiplist) (n int) {
	x := s.NewIterator()
	for x.Next() {
		n++
	}
	return n
}

// compact compacts a MemDB.
func compact(s *Skiplist) (*Skiplist, error) {
	n := New(nil)
	x := s.NewIterator()
	for x.Next() {
		if err := n.Set(x.Key(), x.Value()); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func TestBasic(t *testing.T) {
	// Check the empty DB.
	m := New(nil)
	if got, want := count(m), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.Get([]byte("cherry"))
	if string(v) != "" || err != ErrKeyNotFound {
		t.Fatalf("1.Get: got (%q, %v), want (%q, %v)", v, err, "", ErrKeyNotFound)
	}
	// Add some key/value pairs.
	m.Set([]byte("cherry"), []byte("red"))
	m.Set([]byte("peach"), []byte("yellow"))
	m.Set([]byte("grape"), []byte("red"))
	m.Set([]byte("grape"), []byte("green"))
	m.Set([]byte("plum"), []byte("purple"))
	if got, want := count(m), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	// Delete a key twice.
	if got, want := m.Delete([]byte("grape")), error(nil); got != want {
		t.Fatalf("3.Delete: got %v, want %v", got, want)
	}
	if got, want := m.Delete([]byte("grape")), ErrKeyNotFound; got != want {
		t.Fatalf("4.Delete: got %v, want %v", got, want)
	}
	if got, want := count(m), 3; got != want {
		t.Fatalf("5.count: got %v, want %v", got, want)
	}
	// Get keys that are and aren't in the DB.
	v, err = m.Get([]byte("plum"))
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.Get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = m.Get([]byte("lychee"))
	if string(v) != "" || err != ErrKeyNotFound {
		t.Fatalf("7.Get: got (%q, %v), want (%q, %v)", v, err, "", ErrKeyNotFound)
	}
	// Check an Iterator.
	s, x := "", m.SubMap([]byte("mango"), nil)
	for x.Next() {
		s += fmt.Sprintf("%s/%s.", x.Key(), x.Value())
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	// Check some more sets and deletes.
	if got, want := m.Delete([]byte("cherry")), error(nil); got != want {
		t.Fatalf("10.Delete: got %v, want %v", got, want)
	}
	if got, want := count(m), 2; got != want {
		t.Fatalf("11.count: got %v, want %v", got, want)
	}
	if err := m.Set([]byte("apricot"), []byte("orange")); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := count(m), 3; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
}

func TestCount(t *testing.T) {
	m := New(nil)
	for i := 0; i < 200; i++ {
		if j := count(m); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.Set([]byte{byte(i)}, nil)
	}
}

func TestEmpty(t *testing.T) {
	m := New(nil)
	if !m.Empty() {
		t.Errorf("got !empty, want empty")
	}
	// Add one key/value pair with an empty key and empty value.
	m.Set(nil, nil)
	if m.Empty() {
		t.Errorf("got empty, want !empty")
	}
}

func Test1000Entries(t *testing.T) {
	// Initialize the DB.
	const N = 1000
	m0 := New(nil)
	for i := 0; i < N; i++ {
		k := []byte(strconv.Itoa(i))
		v := []byte(strings.Repeat("x", i))
		m0.Set(k, v)
	}
	// Delete one third of the entries, update another third,
	// and leave the last third alone.
	for i := 0; i < N; i++ {
		switch i % 3 {
		case 0:
			k := []byte(strconv.Itoa(i))
			m0.Delete(k)
		case 1:
			k := []byte(strconv.Itoa(i))
			v := []byte(strings.Repeat("y", i))
			m0.Set(k, v)
		case 2:
			// No-op.
		}
	}
	// Check the DB count.
	if got, want := count(m0), 666; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	// Check random-access lookup.
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 3*N; i++ {
		j := r.Intn(N)
		k := []byte(strconv.Itoa(j))
		v, err := m0.Get(k)
		if len(v) != cap(v) {
			t.Fatalf("Get: j=%d, got len(v)=%d, cap(v)=%d", j, len(v), cap(v))
		}
		var c uint8
		if len(v) != 0 {
			c = v[0]
		}
		switch j % 3 {
		case 0:
			if err != ErrKeyNotFound {
				t.Fatalf("Get: j=%d, got err=%v, want %v", j, err, ErrKeyNotFound)
			}
		case 1:
			if len(v) != j || c != 'y' {
				t.Fatalf("Get: j=%d, got len(v),c=%d,%c, want %d,%c", j, len(v), c, j, 'y')
			}
		case 2:
			if len(v) != j || c != 'x' {
				t.Fatalf("Get: j=%d, got len(v),c=%d,%c, want %d,%c", j, len(v), c, j, 'x')
			}
		}
	}
	// Check that iterating through the middle of the DB looks OK.
	// Keys are in lexicographic order, not numerical order.
	// Multiples of 3 are not present.
	wants := []string{
		"499",
		"5",
		"50",
		"500",
		"502",
		"503",
		"505",
		"506",
		"508",
		"509",
		"511",
	}
	x := m0.SubMap([]byte(wants[0]), nil)
	for _, want := range wants {
		if !x.Next() {
			t.Fatalf("iter: Next failed, want=%q", want)
		}
		if got := string(x.Key()); got != want {
			t.Fatalf("iter: got %q, want %q", got, want)
		}
		if k := x.Key(); len(k) != cap(k) {
			t.Fatalf("iter: len(k)=%d, cap(k)=%d", len(k), cap(k))
		}
		if v := x.Value(); len(v) != cap(v) {
			t.Fatalf("iter: len(v)=%d, cap(v)=%d", len(v), cap(v))
		}
	}
	// Check that compaction reduces memory usage by at least one third.
	amu0 := m0.ApproximateMemoryUsage()
	if amu0 == 0 {
		t.Fatalf("compact: memory usage is zero")
	}
	m1, err := compact(m0)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	amu1 := m1.ApproximateMemoryUsage()
	if ratio := float64(amu1) / float64(amu0); ratio > 0.667 {
		t.Fatalf("compact: memory usage before=%d, after=%d, ratio=%f", amu0, amu1, ratio)
	}
}