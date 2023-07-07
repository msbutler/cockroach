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
	"context"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type batchManager struct {
	batch          streampb.StreamEvent_Batch
	size           int
	spanCfgDecoder cdcevent.Decoder
	appTenantID    roachpb.TenantID
}

func makeBatchManager(decoder cdcevent.Decoder, appTenantID roachpb.TenantID) batchManager {
	return batchManager{
		batch:          streampb.StreamEvent_Batch{},
		spanCfgDecoder: decoder,
		appTenantID:    appTenantID,
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

func (bm *batchManager) addKVAsSpanConfig(ctx context.Context, kv *roachpb.KeyValue) error {
	row, err := bm.spanCfgDecoder.DecodeKV(ctx, *kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return err
	}
	// We assume the system.Span_Configurations schema is
	// CREATE TABLE system.span_configurations (
	//	start_key    BYTES NOT NULL,
	//  end_key      BYTES NOT NULL,
	//	config        BYTES NOT NULL,
	//	CONSTRAINT "primary" PRIMARY KEY (start_key),
	//	CONSTRAINT check_bounds CHECK (start_key < end_key),
	//	FAMILY "primary" (start_key, end_key, config)
	// )
	//
	// God forbid we update the span config system table schema, but in the
	// unlikely event we do, we will have to version gate this decoding step.

	colsAsBytes := make([]tree.DBytes, 3)
	err = row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {

		decoded, ok := tree.AsDBytes(d)
		if !ok {
			return errors.Newf("could not decode %s datum as DBytes datum", d)
		}
		colsAsBytes = append(colsAsBytes, decoded)
		return nil
	})
	if err != nil {
		return err
	}
	span := roachpb.Span{
		Key:    []byte(colsAsBytes[0]),
		EndKey: []byte(colsAsBytes[1]),
	}
	_, tenantID, err := keys.DecodeTenantPrefix(span.Key)
	if !tenantID.Equal(bm.appTenantID) {
		// Don't send span config updates for other tenants.
		return nil
	}

	var conf roachpb.SpanConfig
	if err := protoutil.Unmarshal([]byte(colsAsBytes[2]), &conf); err != nil {
		return err
	}

	rec, err := spanconfig.MakeRecord(spanconfig.DecodeTarget(span), conf)
	if err != nil {
		return err
	}
	spanCfg := roachpb.SpanConfigEntry{
		Target: rec.GetTarget().ToProto(),
		Config: rec.GetConfig(),
	}

	bm.batch.SpanConfigs = append(bm.batch.SpanConfigs, spanCfg)
	bm.size += spanCfg.Size()
	return nil
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
