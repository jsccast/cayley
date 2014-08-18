package rocksdb

import (
	"fmt"
)

// https://github.com/facebook/rocksdb/blob/master/include/rocksdb/statistics.h

var stats = []string{
	"rocksdb.block.cache.miss",
	"rocksdb.block.cache.hit",
	"rocksdb.block.cache.add",
	"rocksdb.block.cache.index.miss",
	"rocksdb.block.cache.index.hit",
	"rocksdb.block.cache.filter.miss",
	"rocksdb.block.cache.filter.hit",
	"rocksdb.block.cache.data.miss",
	"rocksdb.block.cache.data.hit",
	"rocksdb.bloom.filter.useful",
	"rocksdb.memtable.hit",
	"rocksdb.memtable.miss",
	"rocksdb.compaction.key.drop.new",
	"rocksdb.compaction.key.drop.obsolete",
	"rocksdb.compaction.key.drop.user",
	"rocksdb.number.keys.written",
	"rocksdb.number.keys.read",
	"rocksdb.number.keys.updated",
	"rocksdb.bytes.written",
	"rocksdb.bytes.read",
	"rocksdb.no.file.closes",
	"rocksdb.no.file.opens",
	"rocksdb.no.file.errors",
	"rocksdb.l0.slowdown.micros",
	"rocksdb.memtable.compaction.micros",
	"rocksdb.l0.num.files.stall.micros",
	"rocksdb.rate.limit.delay.millis",
	"rocksdb.num.iterators",
	"rocksdb.number.multiget.get",
	"rocksdb.number.multiget.keys.read",
	"rocksdb.number.multiget.bytes.read",
	"rocksdb.number.deletes.filtered",
	"rocksdb.number.merge.failures",
	"rocksdb.sequence.number",
	"rocksdb.bloom.filter.prefix.checked",
	"rocksdb.bloom.filter.prefix.useful",
	"rocksdb.number.reseeks.iteration",
	"rocksdb.getupdatessince.calls",
	"rocksdb.block.cachecompressed.miss",
	"rocksdb.block.cachecompressed.hit",
	"rocksdb.wal.synced",
	"rocksdb.wal.bytes",
	"rocksdb.write.self",
	"rocksdb.write.other",
	"rocksdb.write.timedout",
	"rocksdb.write.wal",
	"rocksdb.flush.write.bytes",
	"rocksdb.compact.read.bytes",
	"rocksdb.compact.write.bytes",
	"rocksdb.number.direct.load.table.properties",
	"rocksdb.number.superversion_acquires",
	"rocksdb.number.superversion_releases",
	"rocksdb.number.superversion_cleanups",
	"rocksdb.number.block.not_compressed",

	// Histograms

	"rocksdb.db.get.micros" ,
	"rocksdb.db.write.micros" ,
	"rocksdb.compaction.times.micros" ,
	"rocksdb.table.sync.micros" ,
	"rocksdb.compaction.outfile.sync.micros" ,
	"rocksdb.wal.file.sync.micros" ,
	"rocksdb.manifest.file.sync.micros" ,
	"rocksdb.table.open.io.micros" ,
	"rocksdb.db.multiget.micros" ,
	"rocksdb.read.block.compaction.micros" ,
	"rocksdb.read.block.get.micros" ,
	"rocksdb.write.raw.block.micros" ,
	"rocksdb.l0.slowdown.count",
	"rocksdb.memtable.compaction.count",
	"rocksdb.num.files.stall.count",
	"rocksdb.hard.rate.limit.delay.count",
	"rocksdb.soft.rate.limit.delay.count",
	"rocksdb.numfiles.in.singlecompaction" ,
}

// rocksdb table_properties.cc
var props = []string {
	"rocksdb.data.size",
	"rocksdb.index.size",
	"rocksdb.filter.size",
	"rocksdb.raw.key.size",
	"rocksdb.raw.value.size",
	"rocksdb.num.data.blocks",
	"rocksdb.num.entries",
	"rocksdb.filter.policy",
	"rocksdb.format.version",
	"rocksdb.fixed.key.length",

	"rocksdb.properties",
	
	// See rocksdb/.../db.h
	"rocksdb.stats",
	"rocksdb.sstables",
	"rocksdb.num-files-at-level0",
	"rocksdb.num-files-at-level1",
	"rocksdb.num-files-at-level2",
}

func (ts *TripleStore) GetStats() string {
	var out string
	// ToDo: Figure out how to get these properties and stats.
	// Not exposed in DanielMorsing/rocksdb:
	// extern void rocksdb_options_enable_statistics(rocksdb_options_t*);

	for _,p := range append(stats,props...) {
		v := ts.db.PropertyValue(p)
		if 0 < len(v) {
			out += fmt.Sprintf("%s %v\n", p, v)
		}
	}
	return out
}

