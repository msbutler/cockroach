// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamproducer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type batchManager struct {
	batch streampb.StreamEvent_Batch
	size  int
}

func makeBatchManager() batchManager {
	return batchManager{
		batch: streampb.StreamEvent_Batch{},
	}
}

func (bm *batchManager) reset() {
	bm.size = 0
	bm.batch.KeyValues = bm.batch.KeyValues[:0]
	bm.batch.Ssts = bm.batch.Ssts[:0]
	bm.batch.DelRanges = bm.batch.DelRanges[:0]
}

func (bm *batchManager) addSST(sst *kvpb.RangeFeedSSTable) {
	bm.batch.Ssts = append(bm.batch.Ssts, *sst)
	bm.size += sst.Size()
}

func (bm *batchManager) addKV(kv *roachpb.KeyValue) {
	bm.batch.KeyValues = append(bm.batch.KeyValues, *kv)
	bm.size += kv.Size()
}

func (bm *batchManager) addDelRange(d *kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	bm.batch.DelRanges = append(bm.batch.DelRanges, *d)
	bm.size += d.Size()
}

func (bm *batchManager) getSize() int {
	return bm.size
}
