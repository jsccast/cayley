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

// This code requires a fork of github.com/DanielMorsing/rocksdb that has more knobs exposed.

import (
	"fmt"
	"github.com/jsccast/cayley/graph"
	rocks "github.com/jsccast/rocksdb"
)

const (
	DefaultCacheSize = 1 << 20
)

// type Options map[string]interface{}

// https://github.com/facebook/rocksdb/blob/master/include/rocksdb/c.h
// https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h

func findOptions(options *graph.Options) *graph.Options {
	if options == nil {
		m := make(graph.Options)
		options = &m
	}
	dbOpts, given := (*options)["rocks_opts"]
	if given {
		opts := dbOpts.(graph.Options)
		options = &opts
	}
	return options
}

func RocksOpts(options *graph.Options) *rocks.Options {

	options = findOptions(options)


	// ToDo
	env := rocks.NewDefaultEnv()

	if n, ok := options.IntKey("background_threads"); ok {
		env.SetBackgroundThreads(n)
		fmt.Printf("config env.SetBackgroundThreads(%d)\n", n)
	}

	if n, ok := options.IntKey("high_priority_background_threads"); ok {
		env.SetHighPriorityBackgroundThreads(n)
		fmt.Printf("config env.SetHighPriorityBackgroundThreads(%d)\n", n)
	}

	opts := rocks.NewOptions()
	opts.SetEnv(env)

	if b, ok := options.BoolKey("read_only"); ok {
		opts.SetReadOnly(b)
		fmt.Printf("config opts.SetReadOnly(%v)\n", b)
	}

	cacheSize := DefaultCacheSize
	if n, ok := options.IntKey("cache_size"); ok {
		cacheSize = n
	}
	cache := rocks.NewLRUCache(cacheSize)
	opts.SetCache(cache)

	// opts.SetComparator(cmp)

	if n, ok := options.IntKey("increase_parallelism"); ok {
		opts.IncreaseParallelism(n)
		fmt.Printf("config opts.IncreaseParallelism(%d)\n", n)
	}

	if b, ok := options.BoolKey("disable_data_sync"); ok {
		opts.SetDisableDataSync(b)
		fmt.Printf("config opts.SetDisableDataSync(%v)\n", b)
	}

	if n, ok := options.IntKey("bytes_per_sync_power"); ok {
		opts.SetBytesPerSync(uint64(1) << uint64(n))
		fmt.Printf("config opts.SetBytesPerSync(%d)\n", uint64(1)<<uint64(n))
	}

	if n, ok := options.IntKey("log_level"); ok {
		opts.SetLogLevel(n)
		fmt.Printf("config opts.SetLogLevel(%d)\n", n)
	}

	if dir, ok := options.StringKey("log_dir"); ok {
		opts.SetLogDir(dir)
		fmt.Printf("config opts.SetLogDir(%s)\n", dir)
	}

	if dir, ok := options.StringKey("wal_dir"); ok {
		opts.SetLogDir(dir)
		fmt.Printf("config opts.SetWalDir(%s)\n", dir)
	}

	if n, ok := options.IntKey("stats_dump_period"); ok {
		opts.SetStatsDumpPeriod(uint(n))
		fmt.Printf("config opts.SetStatsDumpPeriod(%d)\n", n)
	}

	if n, ok := options.IntKey("write_buffer_size"); ok {
		opts.SetWriteBufferSize(n)
		fmt.Printf("config opts.SetWriteBufferSize(%d)\n", n)
	}

	if n, ok := options.IntKey("write_buffer_size_power"); ok {
		opts.SetWriteBufferSize(int(uint64(1) << uint64(n)))
		fmt.Printf("config opts.SetWriteBufferSize(%d)\n", int(uint64(1)<<uint(n)))
	}

	if b, ok := options.BoolKey("paranoid_checks"); ok {
		opts.SetParanoidChecks(b)
		fmt.Printf("config opts.SetParanoidChecks(%v)\n", b)
	}

	if b, ok := options.BoolKey("allow_mmap_reads"); ok {
		opts.SetAllowMMapReads(b)
		fmt.Printf("config opts.SetAllowMMapReads(%v)\n", b)
	}

	if b, ok := options.BoolKey("allow_mmap_writes"); ok {
		opts.SetAllowMMapWrites(b)
		fmt.Printf("config opts.SetAllowMMapWrites(%v)\n", b)
	}

	if b, ok := options.BoolKey("allow_os_buffer"); ok {
		opts.SetAllowOSBuffer(b)
		fmt.Printf("config opts.SetAllowOSBuffer(%v)\n", b)
	}

	if n, ok := options.IntKey("max_open_files"); ok {
		opts.SetMaxOpenFiles(n)
		fmt.Printf("config opts.SetMaxOpenFiles(%d)\n", n)
	}

	if n, ok := options.IntKey("max_write_buffer_number"); ok {
		opts.SetMaxWriteBufferNumber(n)
		fmt.Printf("config opts.SetMaxWriteBufferNumber(%d)\n", n)
	}

	if n, ok := options.IntKey("min_write_buffer_number_to_merge"); ok {
		opts.SetMinWriteBufferNumberToMerge(n)
		fmt.Printf("config opts.SetMinWriteBufferNumberToMerge(%d)\n", n)
	}

	if n, ok := options.IntKey("block_size"); ok {
		opts.SetBlockSize(n)
		fmt.Printf("config opts.SetBlockSize(%d)\n", n)
	}

	if n, ok := options.IntKey("block_restart_interval"); ok {
		opts.SetBlockRestartInterval(n)
		fmt.Printf("config opts.SetBlockRestartInterval(%d)\n", n)
	}

	// Compaction

	if n, ok := options.IntKey("num_levels"); ok {
		opts.SetNumLevels(n)
		fmt.Printf("config opts.SetNumLevels(%d)\n", n)
	}

	if n, ok := options.IntKey("level0_num_file_compaction_trigger"); ok {
		opts.SetLevel0FileNumCompactionTrigger(n)
		fmt.Printf("config opts.SetLevel0FileNumCompactionTrigger(%d)\n", n)
	}

	if n, ok := options.IntKey("target_file_size_base_power"); ok {
		opts.SetTargetFileSizeBase(uint64(1) << uint64(n))
		fmt.Printf("config opts.SetTargetFileSizeBase(%d)\n", uint64(1)<<uint64(n))
	}

	if n, ok := options.IntKey("target_file_size_multiplier"); ok {
		opts.SetTargetFileSizeMultiplier(n)
		fmt.Printf("config opts.SetTargetFileSizeMultiplier(%d)\n", n)
	}

	if n, ok := options.IntKey("max_background_compactions"); ok {
		opts.SetMaxBackgroundCompactions(n)
		fmt.Printf("config opts.SetMaxBackgroundCompactions(%d)\n", n)
	}

	if n, ok := options.IntKey("max_background_flushes"); ok {
		opts.SetMaxBackgroundFlushes(n)
		fmt.Printf("config opts.SetMaxBackgroundFlushes(%d)\n", n)
	}

	comp, ok := options.StringKey("compression")
	if !ok {
		opts.SetCompression(rocks.NoCompression)
	} else {
		// ToDo: https://github.com/facebook/rocksdb/blob/master/include/rocksdb/c.h#L520-L527
		switch comp {
		case "snappy":
			opts.SetCompression(rocks.SnappyCompression)
		case "none":
			opts.SetCompression(rocks.NoCompression)
		default:
			panic(fmt.Errorf("Bad compression: %s", comp))
			return nil
		}
	}

	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(false)

	return opts
}

func RocksReadOpts(options *graph.Options) *rocks.ReadOptions {
	// ToDo
	options = findOptions(options)
	opts := rocks.NewReadOptions()

	if b, ok := options.BoolKey("verify_checksums"); ok {
		opts.SetVerifyChecksums(b)
		fmt.Printf("config opts.SetVerifyChecksums(%v)\n", b)
	}
	if b, ok := options.BoolKey("fill_cache_size"); ok {
		opts.SetFillCache(b)
		fmt.Printf("config opts.SetFillCache(%v)\n", b)
	}

	return opts
}

func RocksWriteOpts(options *graph.Options) *rocks.WriteOptions {
	// ToDo
	options = findOptions(options)
	opts := rocks.NewWriteOptions()

	if b, ok := options.BoolKey("sync"); ok {
		opts.SetSync(b)
		fmt.Printf("config opts.SetSync(%v)\n", b)
	}

	if b, ok := options.BoolKey("disable_wal"); ok {
		opts.DisableWAL(b)
		fmt.Printf("config opts.DisableWAL(%v)\n", b)
	}
	return opts
}

