// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type batcher struct {
	batch streampb.StreamEvent_Batch
	size  int
}

func makeBatcher() *batcher {
	return &batcher{
		batch: streampb.StreamEvent_Batch{},
	}
}

func (b *batcher) reset() {
	b.size = 0
	b.batch.KeyValues = b.batch.KeyValues[:0]
	b.batch.Ssts = b.batch.Ssts[:0]
	b.batch.DelRanges = b.batch.DelRanges[:0]
}

func (b *batcher) addSST(sst *kvpb.RangeFeedSSTable) {
	b.batch.Ssts = append(b.batch.Ssts, *sst)
	b.size += sst.Size()
}

func (b *batcher) addKV(kv *roachpb.KeyValue) {
	b.batch.KeyValues = append(b.batch.KeyValues, *kv)
	b.size += kv.Size()
}

func (b *batcher) addDelRange(d *kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	b.batch.DelRanges = append(b.batch.DelRanges, *d)
	b.size += d.Size()
}

func (b *batcher) getSize() int {
	return b.size
}
