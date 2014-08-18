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
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/graph/iterator"
	"github.com/jsccast/cayley/quad"
)

func makeTripleSet() []quad.Quad {
	tripleSet := []quad.Quad{
		{"A", "follows", "B", ""},
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
		{"D", "follows", "B", ""},
		{"B", "follows", "F", ""},
		{"F", "follows", "G", ""},
		{"D", "follows", "G", ""},
		{"E", "follows", "F", ""},
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	return tripleSet
}

func iteratedTriples(qs graph.TripleStore, it graph.Iterator) []quad.Quad {
	var res ordered
	for graph.Next(it) {
		res = append(res, qs.Quad(it.Result()))
	}
	sort.Sort(res)
	return res
}

type ordered []quad.Quad

func (o ordered) Len() int { return len(o) }
func (o ordered) Less(i, j int) bool {
	switch {
	case o[i].Subject < o[j].Subject,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate < o[j].Predicate,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object < o[j].Object,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object == o[j].Object &&
			o[i].Label < o[j].Label:

		return true

	default:
		return false
	}
}
func (o ordered) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func iteratedNames(qs graph.TripleStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		res = append(res, qs.NameOf(it.Result()))
	}
	sort.Strings(res)
	return res
}

func TestCreateDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpDir)

	err = createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create Rocksdb database.")
	}

	qs, err := newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	err = createNewRocksdb("/dev/null/some terrible path", nil)
	if err == nil {
		t.Errorf("Created Rocksdb database for bad path.")
	}

	os.RemoveAll(tmpDir)
}

func TestLoadDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	err = createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create Rocksdb database.")
	}

	qs, err := newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}

	qs.AddTriple(quad.Quad{"Something", "points_to", "Something Else", "context"})
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := qs.NameOf(qs.ValueOf(pq)); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := qs.Size(); s != 1 {
		t.Errorf("Unexpected triplestore size, got:%d expect:1", s)
	}
	qs.Close()

	err = createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create Rocksdb database.")
	}
	qs, err = newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}

	ts2, didConvert := qs.(*TripleStore)
	if !didConvert {
		t.Errorf("Could not convert from generic to Rocksdb TripleStore")
	}

	qs.AddTripleSet(makeTripleSet())
	if s := qs.Size(); s != 11 {
		t.Errorf("Unexpected triplestore size, got:%d expect:11", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 5 {
		t.Errorf("Unexpected triplestore size, got:%d expect:5", s)
	}

	qs.RemoveTriple(quad.Quad{"A", "follows", "B", ""})
	if s := qs.Size(); s != 10 {
		t.Errorf("Unexpected triplestore size after RemoveTriple, got:%d expect:10", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 4 {
		t.Errorf("Unexpected triplestore size, got:%d expect:4", s)
	}

	qs.Close()
}

func TestIterator(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	err = createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create Rocksdb database.")
	}

	qs, err := newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}
	qs.AddTripleSet(makeTripleSet())
	var it graph.Iterator

	it = qs.NodesAllIterator()
	if it == nil {
		t.Fatal("Got nil iterator.")
	}

	size, exact := it.Size()
	if size <= 0 || size >= 20 {
		t.Errorf("Unexpected size, got:%d expect:(0, 20)", size)
	}
	if exact {
		t.Errorf("Got unexpected exact result.")
	}
	if typ := it.Type(); typ != graph.All {
		t.Errorf("Unexpected iterator type, got:%v expect:%v", typ, graph.All)
	}
	optIt, changed := it.Optimize()
	if changed || optIt != it {
		t.Errorf("Optimize unexpectedly changed iterator.")
	}

	expect := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := iteratedNames(qs, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Unexpected iterated result on repeat %d, got:%v expect:%v", i, got, expect)
		}
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Contains(qs.ValueOf(pq)) {
			t.Errorf("Failed to find and check %q correctly", pq)
		}
	}
	// FIXME(kortschak) Why does this fail?
	/*
		for _, pq := range []string{"baller"} {
			if it.Contains(qs.ValueOf(pq)) {
				t.Errorf("Failed to check %q correctly", pq)
			}
		}
	*/
	it.Reset()

	it = qs.TriplesAllIterator()
	graph.Next(it)
	triple := qs.Quad(it.Result())
	set := makeTripleSet()
	var ok bool
	for _, t := range set {
		if t.String() == triple.String() {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Failed to find %q during iteration, got:%q", triple, set)
	}

	qs.Close()
}

func TestSetIterator(t *testing.T) {

	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	err := createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}

	qs, err := newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}
	defer qs.Close()

	qs.AddTripleSet(makeTripleSet())

	expect := []quad.Quad{
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
	}
	sort.Sort(ordered(expect))

	// Subject iterator.
	it := qs.TripleIterator(quad.Subject, qs.ValueOf("C"))

	if got := iteratedTriples(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	and := iterator.NewAnd()
	and.AddSubIterator(qs.TriplesAllIterator())
	and.AddSubIterator(it)

	if got := iteratedTriples(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Object iterator.
	it = qs.TripleIterator(quad.Object, qs.ValueOf("F"))

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
		{"E", "follows", "F", ""},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}

	and = iterator.NewAnd()
	and.AddSubIterator(qs.TripleIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
	}
	if got := iteratedTriples(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Predicate iterator.
	it = qs.TripleIterator(quad.Predicate, qs.ValueOf("status"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}

	// Label iterator.
	it = qs.TripleIterator(quad.Label, qs.ValueOf("status_graph"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd()
	and.AddSubIterator(qs.TripleIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedTriples(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd()
	and.AddSubIterator(it)
	and.AddSubIterator(qs.TripleIterator(quad.Subject, qs.ValueOf("B")))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedTriples(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
}

func TestOptimize(t *testing.T) {
	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	err := createNewRocksdb(tmpDir, nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}
	qs, err := newTripleStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create rocksdb TripleStore.")
	}
	qs.AddTripleSet(makeTripleSet())

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf("F"))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newTriples := iteratedTriples(qs, newIt)
	oldTriples := iteratedTriples(qs, oldIt)
	if !reflect.DeepEqual(newTriples, oldTriples) {
		t.Errorf("Optimized iteration does not match original")
	}

	graph.Next(oldIt)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	graph.Next(newIt)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}
