// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func setupMVCCRocksDB(b testing.TB, dir string) Engine {
	cache := NewRocksDBCache(1 << 30 /* 1GB */)
	defer cache.Release()

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		cache,
	)
	if err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", dir, err)
	}
	return rocksdb
}

func setupMVCCInMemRocksDB(_ testing.TB, loc string) Engine {
	return NewInMem(roachpb.Attributes{}, testCacheSize)
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(b, setupMVCCRocksDB, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: false,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCReverseScan_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(b, setupMVCCRocksDB, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: true,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCGet_RocksDB(b *testing.B) {
	for _, numVersions := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, valueSize := range []int{8} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCGet(b, setupMVCCRocksDB, benchDataOptions{
						numVersions: numVersions,
						valueBytes:  valueSize,
					})
				})
			}
		})
	}
}

func BenchmarkMVCCComputeStats_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCComputeStats(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCFindSplitKey_RocksDB(b *testing.B) {
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkIterOnBatch_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnBatch(b, writes)
		})
	}
}

// BenchmarkIterOnReadOnly_RocksDB is a microbenchmark that measures the performance of creating an iterator
// and seeking to a key if a read-only ReadWriter that caches the RocksDB iterator is used
func BenchmarkIterOnReadOnly_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnReadWriter(b, writes, Engine.NewReadOnly, true)
		})
	}
}

// BenchmarkIterOnEngine_RocksDB is a microbenchmark that measures the performance of creating an iterator
// and seeking to a key without caching is used (see BenchmarkIterOnReadOnly_RocksDB)
func BenchmarkIterOnEngine_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnReadWriter(b, writes, func(e Engine) ReadWriter { return e }, false)
		})
	}
}

// Write benchmarks. Most of them run in-memory except for DeleteRange benchs,
// which make more sense when data is present.

func BenchmarkMVCCPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut_RocksDB(b *testing.B) {
	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCConditionalPut(b, setupMVCCInMemRocksDB, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCInitPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindInitPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBatchPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range []int{1, 100, 10000, 100000} {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					runMVCCBatchPut(b, setupMVCCInMemRocksDB, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries_RocksDB(b *testing.B) {
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(b, setupMVCCInMemRocksDB, batchSize)
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).

func BenchmarkMVCCDeleteRange_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkBatchApplyBatchRepr(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, writeOnly := range []bool{false, true} {
		b.Run(fmt.Sprintf("writeOnly=%t ", writeOnly), func(b *testing.B) {
			for _, valueSize := range []int{10} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					for _, batchSize := range []int{1000000} {
						b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
							runBatchApplyBatchRepr(b, setupMVCCInMemRocksDB, writeOnly, valueSize, batchSize)
						})
					}
				})
			}
		})
	}
}

func BenchmarkBatchBuilderPut(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	b.ResetTimer()

	const batchSize = 1000
	batch := &RocksDBBatchBuilder{}
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: int64(j)}
			batch.Put(MVCCKey{key, ts}, value)
		}
		batch.Finish()
	}

	b.StopTimer()
}
