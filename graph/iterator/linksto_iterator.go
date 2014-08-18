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

// Defines one of the base iterators, the LinksTo iterator. A LinksTo takes a
// subiterator of nodes, and contains an iteration of links which "link to"
// those nodes in a given direction.
//
// Next()ing a LinksTo is straightforward -- iterate through all links to //
// things in the subiterator, and then advance the subiterator, and do it again.
// LinksTo is therefore sensitive to growing with a fanout. (A small-sized
// subiterator could cause LinksTo to be large).
//
// Contains()ing a LinksTo means, given a link, take the direction we care about
// and check if it's in our subiterator. Checking is therefore fairly cheap, and
// similar to checking the subiterator alone.
//
// Can be seen as the dual of the HasA iterator.

import (
	"fmt"
	"strings"

	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/quad"
)

// A LinksTo has a reference back to the graph.TripleStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type LinksTo struct {
	uid       uint64
	tags      graph.Tagger
	ts        graph.TripleStore
	primaryIt graph.Iterator
	dir       quad.Direction
	nextIt    graph.Iterator
	result    graph.Value
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func NewLinksTo(ts graph.TripleStore, it graph.Iterator, d quad.Direction) *LinksTo {
	return &LinksTo{
		uid:       NextUID(),
		ts:        ts,
		primaryIt: it,
		dir:       d,
		nextIt:    &Null{},
	}
}

func (it *LinksTo) UID() uint64 {
	return it.uid
}

func (it *LinksTo) Reset() {
	it.primaryIt.Reset()
	if it.nextIt != nil {
		it.nextIt.Close()
	}
	it.nextIt = &Null{}
}

func (it *LinksTo) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *LinksTo) Clone() graph.Iterator {
	out := NewLinksTo(it.ts, it.primaryIt.Clone(), it.dir)
	out.tags.CopyFrom(it)
	return out
}

// Return the direction under consideration.
func (it *LinksTo) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *LinksTo) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.primaryIt.TagResults(dst)
}

// DEPRECATED
func (it *LinksTo) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.primaryIt.ResultTree())
	return tree
}

// Print the iterator.
func (it *LinksTo) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s %d direction:%s\n%s)",
		strings.Repeat(" ", indent),
		it.Type(), it.UID(), it.dir, it.primaryIt.DebugString(indent+4))
}

// If it checks in the right direction for the subiterator, it is a valid link
// for the LinksTo.
func (it *LinksTo) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	node := it.ts.TripleDirection(val, it.dir)
	if it.primaryIt.Contains(node) {
		it.result = val
		return graph.ContainsLogOut(it, val, true)
	}
	return graph.ContainsLogOut(it, val, false)
}

// Return a list containing only our subiterator.
func (it *LinksTo) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksTo) Optimize() (graph.Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if it.primaryIt.Type() == graph.Null {
			it.nextIt.Close()
			return it.primaryIt, true
		}
	}
	// Ask the graph.TripleStore if we can be replaced. Often times, this is a great
	// optimization opportunity (there's a fixed iterator underneath us, for
	// example).
	newReplacement, hasOne := it.ts.OptimizeIterator(it)
	if hasOne {
		it.Close()
		return newReplacement, true
	}
	return it, false
}

// Next()ing a LinksTo operates as described above.
func (it *LinksTo) Next() bool {
	graph.NextLogIn(it)
	if graph.Next(it.nextIt) {
		it.result = it.nextIt.Result()
		return graph.NextLogOut(it, it.nextIt, true)
	}

	// Subiterator is empty, get another one
	if !graph.Next(it.primaryIt) {
		// We're out of nodes in our subiterator, so we're done as well.
		return graph.NextLogOut(it, 0, false)
	}
	it.nextIt.Close()
	it.nextIt = it.ts.TripleIterator(it.dir, it.primaryIt.Result())

	// Recurse -- return the first in the next set.
	return it.Next()
}

func (it *LinksTo) Result() graph.Value {
	return it.result
}

// Close our subiterators.
func (it *LinksTo) Close() {
	it.nextIt.Close()
	it.primaryIt.Close()
}

// We won't ever have a new result, but our subiterators might.
func (it *LinksTo) NextPath() bool {
	return it.primaryIt.NextPath()
}

// Register the LinksTo.
func (it *LinksTo) Type() graph.Type { return graph.LinksTo }

// Return a guess as to how big or costly it is to next the iterator.
func (it *LinksTo) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the triplestore itself
	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)
	return graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         fanoutFactor * subitStats.Size,
	}
}

func (it *LinksTo) Size() (int64, bool) {
	return 0, true
}
