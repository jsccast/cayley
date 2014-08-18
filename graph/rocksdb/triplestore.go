// Portions Copyright 2014 Comcast Cable Communications Management, LLC
// Portions Copyright 2014 The Cayley Authors. All rights reserved.
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
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"hash"
	"sync"

	"github.com/barakmich/glog"
	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/graph/iterator"
	"github.com/jsccast/cayley/quad"

	rocks "github.com/jsccast/rocksdb"
)

var rocksErrNotFound error = nil

func init() {
	graph.RegisterTripleStore("rocksdb", true, NewTripleStore, CreateNewRocksDB)
}

const (
	// DefaultCacheSize       = 2
	DefaultWriteBufferSize = 20
)

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize = sha1.Size
)

type Token []byte

func (t Token) Key() interface{} {
	return string(t)
}

type TripleStore struct {
	dbOpts    *rocks.Options
	db        *rocks.DB
	path      string
	open      bool
	size      int64
	writeopts *rocks.WriteOptions
	readopts  *rocks.ReadOptions
}

func CreateNewRocksDB(path string, options graph.Options) error {
	opts := RocksOpts(&options)
	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(false) // ToDo
	db, err := rocks.Open(path, opts)
	if err != nil {
		glog.Errorln("Error: couldn't create database: ", err)
		return err
	}

	ts := &TripleStore{}
	ts.db = db
	wopts := RocksWriteOpts(&options)
	wopts.SetSync(true)
	ts.writeopts = wopts
	ts.Close()
	return nil
}

func NewTripleStore(path string, options graph.Options) (graph.TripleStore,error) {
	var ts TripleStore
	ts.path = path
	opts := RocksOpts(&options)

	db, err := rocks.Open(ts.path, opts)
	if err != nil {
		return &ts, err
	}
	ts.db = db
	
	// ts.hasher = sha1.New()
	ts.readopts = RocksReadOpts(&options)
	ts.writeopts = RocksWriteOpts(&options)
	// glog.Error(ts.GetStats())

	ts.getSize()
	
	return &ts, nil
}

func (qs *TripleStore) Size() int64 {
	return qs.size
}

func (qs *TripleStore) createKeyFor(d [3]quad.Direction, triple quad.Quad) []byte {
	key := make([]byte, 0, 2+(hashSize*3))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{d[0].Prefix(), d[1].Prefix()}...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[0]))...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[1]))...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[2]))...)
	return key
}

func (qs *TripleStore) createProvKeyFor(d [3]quad.Direction, triple quad.Quad) []byte {
	key := make([]byte, 0, 2+(hashSize*4))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{quad.Label.Prefix(), d[0].Prefix()}...)
	key = append(key, qs.convertStringToByteHash(triple.Get(quad.Label))...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[0]))...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[1]))...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[2]))...)
	return key
}

func (qs *TripleStore) createValueKeyFor(s string) []byte {
	key := make([]byte, 0, 1+hashSize)
	key = append(key, []byte("z")...)
	key = append(key, qs.convertStringToByteHash(s)...)
	return key
}

func (qs *TripleStore) AddTriple(t quad.Quad) {
	batch := &rocks.WriteBatch{}
	qs.buildWrite(batch, t)
	err := qs.db.Write(qs.writeopts, batch)
	if err != nil {
		glog.Errorf("Couldn't write to DB for triple %s.", t)
		return
	}
	qs.size++
}

// Short hand for direction permutations.
var (
	spo = [3]quad.Direction{quad.Subject, quad.Predicate, quad.Object}
	osp = [3]quad.Direction{quad.Object, quad.Subject, quad.Predicate}
	pos = [3]quad.Direction{quad.Predicate, quad.Object, quad.Subject}
	pso = [3]quad.Direction{quad.Predicate, quad.Subject, quad.Object}
)

func (qs *TripleStore) RemoveTriple(t quad.Quad) {
	_, err := qs.db.Get(qs.readopts, qs.createKeyFor(spo, t))
	if err != nil && err != rocksErrNotFound {
		glog.Error("Couldn't access DB to confirm deletion")
		return
	}

	batch := &rocks.WriteBatch{}
	batch.Delete(qs.createKeyFor(spo, t))
	batch.Delete(qs.createKeyFor(osp, t))
	batch.Delete(qs.createKeyFor(pos, t))
	qs.UpdateValueKeyBy(t.Get(quad.Subject), -1, batch)
	qs.UpdateValueKeyBy(t.Get(quad.Predicate), -1, batch)
	qs.UpdateValueKeyBy(t.Get(quad.Object), -1, batch)
	if t.Get(quad.Label) != "" {
		batch.Delete(qs.createProvKeyFor(pso, t))
		qs.UpdateValueKeyBy(t.Get(quad.Label), -1, batch)
	}
	err = qs.db.Write(nil, batch)
	if err != nil {
		glog.Errorf("Couldn't delete triple %s.", t)
		return
	}
	qs.size--
}

func (qs *TripleStore) buildTripleWrite(batch *rocks.WriteBatch, t quad.Quad) {
	bytes, err := json.Marshal(t)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for triple %s: %s", t, err)
		return
	}
	batch.Put(qs.createKeyFor(spo, t), bytes)
	batch.Put(qs.createKeyFor(osp, t), bytes)
	batch.Put(qs.createKeyFor(pos, t), bytes)
	if t.Get(quad.Label) != "" {
		batch.Put(qs.createProvKeyFor(pso, t), bytes)
	}
}

func (qs *TripleStore) buildWrite(batch *rocks.WriteBatch, t quad.Quad) {
	qs.buildTripleWrite(batch, t)
	qs.UpdateValueKeyBy(t.Get(quad.Subject), 1, nil)
	qs.UpdateValueKeyBy(t.Get(quad.Predicate), 1, nil)
	qs.UpdateValueKeyBy(t.Get(quad.Object), 1, nil)
	if t.Get(quad.Label) != "" {
		qs.UpdateValueKeyBy(t.Get(quad.Label), 1, nil)
	}
}

type ValueData struct {
	Name string
	Size int64
}

func (qs *TripleStore) UpdateValueKeyBy(name string, amount int, batch *rocks.WriteBatch) {
	value := &ValueData{name, int64(amount)}
	key := qs.createValueKeyFor(name)
	b, err := qs.db.Get(qs.readopts, key)

	// Error getting the node from the database.
	if err != nil && err != rocksErrNotFound {
		glog.Errorf("Error reading Value %s from the DB.", name)
		return
	}

	// Node exists in the database -- unmarshal and update.
	if b != nil && err != rocksErrNotFound {
		err = json.Unmarshal(b, value)
		if err != nil {
			glog.Errorf("Error: couldn't reconstruct value: %v", err)
			return
		}
		value.Size += int64(amount)
	}

	// Are we deleting something?
	if amount < 0 {
		if value.Size <= 0 {
			if batch == nil {
				qs.db.Delete(qs.writeopts, key)
			} else {
				batch.Delete(key)
			}
			return
		}
	}

	// Repackage and rewrite.
	bytes, err := json.Marshal(&value)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for value %s: %s", name, err)
		return
	}
	if batch == nil {
		qs.db.Put(qs.writeopts, key, bytes)
	} else {
		batch.Put(key, bytes)
	}
}

func (qs *TripleStore) AddTripleSet(t_s []quad.Quad) {
	batch := rocks.NewWriteBatch()
	newTs := len(t_s)
	resizeMap := make(map[string]int)
	for _, t := range t_s {
		qs.buildTripleWrite(batch, t)
		resizeMap[t.Subject]++
		resizeMap[t.Predicate]++
		resizeMap[t.Object]++
		if t.Label != "" {
			resizeMap[t.Label]++
		}
	}
	for k, v := range resizeMap {
		qs.UpdateValueKeyBy(k, v, batch)
	}
	err := qs.db.Write(qs.writeopts, batch)
	if err != nil {
		glog.Error("Couldn't write to DB for tripleset.")
		return
	}
	qs.size += int64(newTs)
}

func (qs *TripleStore) Close() {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err == nil {
		werr := qs.db.Put(qs.writeopts, []byte("__size"), buf.Bytes())
		if werr != nil {
			glog.Error("Couldn't write size before closing!")
		}
	} else {
		glog.Errorf("Couldn't convert size before closing!")
	}
	qs.db.Close()
	qs.open = false
}

func (qs *TripleStore) Quad(k graph.Value) quad.Quad {
	var triple quad.Quad
	b, err := qs.db.Get(qs.readopts, k.(Token))
	if err != nil && err != rocksErrNotFound {
		glog.Error("Error: couldn't get triple from DB.")
		return quad.Quad{}
	}
	if err == rocksErrNotFound {
		// No harm, no foul.
		return quad.Quad{}
	}
	err = json.Unmarshal(b, &triple)
	if err != nil {
		glog.Error("Error: couldn't reconstruct triple.")
		return quad.Quad{}
	}
	return triple
}

func (qs *TripleStore) convertStringToByteHash(s string) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return key
}

func (qs *TripleStore) ValueOf(s string) graph.Value {
	return Token(qs.createValueKeyFor(s))
}

func (qs *TripleStore) valueData(value_key []byte) ValueData {
	var out ValueData
	if glog.V(3) {
		glog.V(3).Infof("%s %v", string(value_key[0]), value_key)
	}
	b, err := qs.db.Get(qs.readopts, value_key)
	if err != nil {
		// ToDo
		glog.Errorln("Error: couldn't get value: " + err.Error())
		return ValueData{}
	}
	if b != nil {
		err = json.Unmarshal(b, &out)
		if err != nil {
			glog.Errorln("Error: couldn't reconstruct value: " + err.Error())
			return ValueData{}
		}
	}
	return out
}

func (qs *TripleStore) NameOf(k graph.Value) string {
	if k == nil {
		glog.V(2).Info("k was nil")
		return ""
	}
	return qs.valueData(k.(Token)).Name
}

func (qs *TripleStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return 0
	}
	return int64(qs.valueData(k.(Token)).Size)
}

func (qs *TripleStore) getSize() {
	var size int64
	b, err := qs.db.Get(qs.readopts, []byte("__size"))
	if err != nil && err != rocksErrNotFound {
		panic("Couldn't read size " + err.Error())
	}
	if err == rocksErrNotFound {
		// Must be a new database. Cool
		qs.size = 0
		return
	}
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &size)
	if err != nil {
		glog.Errorln("Error: couldn't parse size")
	}
	qs.size = size
}

func (qs *TripleStore) SizeOfPrefix(pre []byte) (int64, error) {
	limit := make([]byte, len(pre))
	copy(limit, pre)
	end := len(limit) - 1
	limit[end]++
	ranges := []rocks.Range{rocks.Range{pre, limit}}
	sizes := qs.db.GetApproximateSizes(ranges)
	return (int64(sizes[0]) >> 6) + 1, nil
}

func (qs *TripleStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
	var prefix string
	switch d {
	case quad.Subject:
		prefix = "sp"
	case quad.Predicate:
		prefix = "po"
	case quad.Object:
		prefix = "os"
	case quad.Label:
		prefix = "cp"
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(prefix, d, val, qs)
}

func (qs *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator("z", quad.Any, qs)
}

func (qs *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator("po", quad.Predicate, qs)
}

func (qs *TripleStore) TripleDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(Token)
	offset := PositionOf(v[0:2], d, qs)
	if offset != -1 {
		return Token(append([]byte("z"), v[offset:offset+hashSize]...))
	} else {
		return Token(qs.Quad(val).Get(d))
	}
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.(Token), b.(Token))
}

func (qs *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareBytes)
}

