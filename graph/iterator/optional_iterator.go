// Copyright 2014 The Cayley Authors. All rights reserved.
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

package iterator

// "Optional" is kind of odd. It's not an iterator in the strictest sense, but
// it's easier to implement as an iterator.
//
// Consider what it means. It means that we have a subconstraint which we do
// not want to constrain the query -- we just want it to return the matching
// subgraph if one matches at all. By analogy to regular expressions, it is the
// '?' operator.
//
// If it were a proper iterator of its own (and indeed, a reasonable refactor
// of this iterator would be to make it such) it would contain an all iterator
// -- all things in the graph. It matches everything (as does the regex "(a)?")

import (
	"fmt"
	"strings"

	"github.com/jsccast/cayley/graph"
)

// An optional iterator has the sub-constraint iterator we wish to be optional
// and whether the last check we received was true or false.
type Optional struct {
	uid       uint64
	tags      graph.Tagger
	subIt     graph.Iterator
	lastCheck bool
	result    graph.Value
}

// Creates a new optional iterator.
func NewOptional(it graph.Iterator) *Optional {
	return &Optional{
		uid:   NextUID(),
		subIt: it,
	}
}

func (it *Optional) UID() uint64 {
	return it.uid
}

func (it *Optional) Reset() {
	it.subIt.Reset()
	it.lastCheck = false
}

func (it *Optional) Close() {
	it.subIt.Close()
}

func (it *Optional) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Optional) Clone() graph.Iterator {
	out := NewOptional(it.subIt.Clone())
	out.tags.CopyFrom(it)
	return out
}

// DEPRECATED
func (it *Optional) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Optional) Result() graph.Value {
	return it.result
}

// An optional iterator only has a next result if, (a) last time we checked
// we had any results whatsoever, and (b) there was another subresult in our
// optional subbranch.
func (it *Optional) NextPath() bool {
	if it.lastCheck {
		return it.subIt.NextPath()
	}
	return false
}

// No subiterators.
func (it *Optional) SubIterators() []graph.Iterator {
	return nil
}

// Contains() is the real hack of this iterator. It always returns true, regardless
// of whether the subiterator matched. But we keep track of whether the subiterator
// matched for results purposes.
func (it *Optional) Contains(val graph.Value) bool {
	checked := it.subIt.Contains(val)
	it.lastCheck = checked
	it.result = val
	return true
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *Optional) TagResults(dst map[string]graph.Value) {
	if it.lastCheck == false {
		return
	}
	it.subIt.TagResults(dst)
}

// Registers the optional iterator.
func (it *Optional) Type() graph.Type { return graph.Optional }

// Prints the optional and it's subiterator.
func (it *Optional) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s tags:%s\n%s)",
		strings.Repeat(" ", indent),
		it.Type(),
		it.tags.Tags(),
		it.subIt.DebugString(indent+4))
}

// There's nothing to optimize for an optional. Optimize the subiterator and
// potentially replace it.
func (it *Optional) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.subIt.Optimize()
	if changed {
		it.subIt.Close()
		it.subIt = newSub
	}
	return it, false
}

// We're only as expensive as our subiterator. Except, we can't be nexted.
func (it *Optional) Stats() graph.IteratorStats {
	subStats := it.subIt.Stats()
	return graph.IteratorStats{
		ContainsCost: subStats.ContainsCost,
		NextCost:     int64(1 << 62),
		Size:         subStats.Size,
	}
}

// If you're empty and you know it, clap your hands.
func (it *Optional) Size() (int64, bool) {
	return 0, true
}
