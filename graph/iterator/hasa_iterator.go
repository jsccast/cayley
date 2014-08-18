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

// Defines one of the base iterators, the HasA iterator. The HasA takes a
// subiterator of links, and acts as an iterator of nodes in the given
// direction. The name comes from the idea that a "link HasA subject" or a "link
// HasA predicate".
//
// HasA is weird in that it may return the same value twice if on the Next()
// path. That's okay -- in reality, it can be viewed as returning the value for
// a new triple, but to make logic much simpler, here we have the HasA.
//
// Likewise, it's important to think about Contains()ing a HasA. When given a
// value to check, it means "Check all predicates that have this value for your
// direction against the subiterator." This would imply that there's more than
// one possibility for the same Contains()ed value. While we could return the
// number of options, it's simpler to return one, and then call NextPath()
// enough times to enumerate the options. (In fact, one could argue that the
// raison d'etre for NextPath() is this iterator).
//
// Alternatively, can be seen as the dual of the LinksTo iterator.

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"

	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/quad"
)

// A HasA consists of a reference back to the graph.TripleStore that it references,
// a primary subiterator, a direction in which the triples for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type HasA struct {
	uid       uint64
	tags      graph.Tagger
	ts        graph.TripleStore
	primaryIt graph.Iterator
	dir       quad.Direction
	resultIt  graph.Iterator
	result    graph.Value
}

// Construct a new HasA iterator, given the triple subiterator, and the triple
// direction for which it stands.
func NewHasA(ts graph.TripleStore, subIt graph.Iterator, d quad.Direction) *HasA {
	return &HasA{
		uid:       NextUID(),
		ts:        ts,
		primaryIt: subIt,
		dir:       d,
	}
}

func (it *HasA) UID() uint64 {
	return it.uid
}

// Return our sole subiterator.
func (it *HasA) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

func (it *HasA) Reset() {
	it.primaryIt.Reset()
	if it.resultIt != nil {
		it.resultIt.Close()
	}
}

func (it *HasA) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *HasA) Clone() graph.Iterator {
	out := NewHasA(it.ts, it.primaryIt.Clone(), it.dir)
	out.tags.CopyFrom(it)
	return out
}

// Direction accessor.
func (it *HasA) Direction() quad.Direction { return it.dir }

// Pass the Optimize() call along to the subiterator. If it becomes Null,
// then the HasA becomes Null (there are no triples that have any directions).
func (it *HasA) Optimize() (graph.Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if it.primaryIt.Type() == graph.Null {
			return it.primaryIt, true
		}
	}
	return it, false
}

// Pass the TagResults down the chain.
func (it *HasA) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.primaryIt.TagResults(dst)
}

// DEPRECATED Return results in a ResultTree.
func (it *HasA) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.primaryIt.ResultTree())
	return tree
}

// Print some information about this iterator.
func (it *HasA) DebugString(indent int) string {
	var tags string
	for _, k := range it.tags.Tags() {
		tags += fmt.Sprintf("%s;", k)
	}
	return fmt.Sprintf("%s(%s %d tags:%s direction:%s\n%s)", strings.Repeat(" ", indent), it.Type(), it.UID(), tags, it.dir, it.primaryIt.DebugString(indent+4))
}

// Check a value against our internal iterator. In order to do this, we must first open a new
// iterator of "triples that have `val` in our direction", given to us by the triple store,
// and then Next() values out of that iterator and Contains() them against our subiterator.
func (it *HasA) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	if glog.V(4) {
		glog.V(4).Infoln("Id is", it.ts.NameOf(val))
	}
	// TODO(barakmich): Optimize this
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.resultIt = it.ts.TripleIterator(it.dir, val)
	return graph.ContainsLogOut(it, val, it.NextContains())
}

// NextContains() is shared code between Contains() and GetNextResult() -- calls next on the
// result iterator (a triple iterator based on the last checked value) and returns true if
// another match is made.
func (it *HasA) NextContains() bool {
	for graph.Next(it.resultIt) {
		link := it.resultIt.Result()
		if glog.V(4) {
			glog.V(4).Infoln("Quad is", it.ts.Quad(link))
		}
		if it.primaryIt.Contains(link) {
			it.result = it.ts.TripleDirection(link, it.dir)
			return true
		}
	}
	return false
}

// Get the next result that matches this branch.
func (it *HasA) NextPath() bool {
	// Order here is important. If the subiterator has a NextPath, then we
	// need do nothing -- there is a next result, and we shouldn't move forward.
	// However, we then need to get the next result from our last Contains().
	//
	// The upshot is, the end of NextPath() bubbles up from the bottom of the
	// iterator tree up, and we need to respect that.
	glog.V(4).Infoln("HASA", it.UID(), "NextPath")
	if it.primaryIt.NextPath() {
		return true
	}
	result := it.NextContains()
	glog.V(4).Infoln("HASA", it.UID(), "NextPath Returns", result, "")
	return result
}

// Next advances the iterator. This is simpler than Contains. We have a
// subiterator we can get a value from, and we can take that resultant triple,
// pull our direction out of it, and return that.
func (it *HasA) Next() bool {
	graph.NextLogIn(it)
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.resultIt = &Null{}

	if !graph.Next(it.primaryIt) {
		return graph.NextLogOut(it, 0, false)
	}
	tID := it.primaryIt.Result()
	val := it.ts.TripleDirection(tID, it.dir)
	it.result = val
	return graph.NextLogOut(it, val, true)
}

func (it *HasA) Result() graph.Value {
	return it.result
}

// GetStats() returns the statistics on the HasA iterator. This is curious. Next
// cost is easy, it's an extra call or so on top of the subiterator Next cost.
// ContainsCost involves going to the graph.TripleStore, iterating out values, and hoping
// one sticks -- potentially expensive, depending on fanout. Size, however, is
// potentially smaller. we know at worst it's the size of the subiterator, but
// if there are many repeated values, it could be much smaller in totality.
func (it *HasA) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the triplestore itself
	// and be optimized.
	faninFactor := int64(1)
	fanoutFactor := int64(30)
	nextConstant := int64(2)
	tripleConstant := int64(1)
	return graph.IteratorStats{
		NextCost:     tripleConstant + subitStats.NextCost,
		ContainsCost: (fanoutFactor * nextConstant) * subitStats.ContainsCost,
		Size:         faninFactor * subitStats.Size,
	}
}

// Close the subiterator, the result iterator (if any) and the HasA.
func (it *HasA) Close() {
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.primaryIt.Close()
}

// Register this iterator as a HasA.
func (it *HasA) Type() graph.Type { return graph.HasA }

func (it *HasA) Size() (int64, bool) {
	return 0, true
}
