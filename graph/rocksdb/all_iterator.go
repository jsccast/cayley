// Portions Copyright 2014 The Cayley Authors. All rights reserved.
// Portions Copyright 2014 Comcast Cable Communications Management, LLC
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

package rocksdb

import (
	"bytes"
	"strings"
	"fmt"

	rocks "github.com/jsccast/rocksdb"

	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/graph/iterator"
	"github.com/jsccast/cayley/quad"
)

type AllIterator struct {
	uid    uint64
	tags   graph.Tagger
	prefix []byte
	dir    quad.Direction
	open   bool
	iter   *rocks.Iterator
	ts     *TripleStore
	ro     *rocks.ReadOptions
	result graph.Value
}

func NewAllIterator(prefix string, d quad.Direction, ts *TripleStore) *AllIterator {
	opts := RocksReadOpts(nil)

	it := AllIterator{
		uid:    iterator.NextUID(),
		ro:     opts,
		iter:   ts.db.NewIterator(opts),
		prefix: []byte(prefix),
		dir:    d,
		open:   true,
		ts:     ts,
	}

	it.iter.Seek(it.prefix)
	if !it.iter.Valid() {
		// FIXME(kortschak) What are the semantics here? Is this iterator usable?
		// If not, we should return nil *Iterator and an error.
		it.open = false
		it.iter.Close()
	}

	return &it
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	if !it.open {
		it.iter = it.ts.db.NewIterator(it.ro)
		it.open = true
	}
	it.iter.Seek(it.prefix)
	if !it.iter.Valid() {
		it.open = false
		it.iter.Close()
	}
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(string(it.prefix), it.dir, it.ts)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Next() bool {
	if !it.open {
		it.result = nil
		return false
	}
	var out []byte
	out = make([]byte, len(it.iter.Key()))
	copy(out, it.iter.Key())
	it.iter.Next()
	if !it.iter.Valid() {
		it.Close()
	}
	if !bytes.HasPrefix(out, it.prefix) {
		it.Close()
		return false
	}
	it.result = Token(out)
	return true
}

func (it *AllIterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *AllIterator) Result() graph.Value {
	return it.result
}

func (it *AllIterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Contains(v graph.Value) bool {
	it.result = v
	return true
}

func (it *AllIterator) Close() {
	if it.open {
		it.iter.Close()
		it.open = false
	}
}

func (it *AllIterator) Size() (int64, bool) {
	size, err := it.ts.SizeOfPrefix(it.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (it *AllIterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s tags: %v rocksdb size:%d %s %p)", strings.Repeat(" ", indent), it.Type(), it.tags.Tags(), size, it.dir, it)
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}
