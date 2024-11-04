// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TODO(ssd): This is a duplicative with the split_and_scatter processor in
// backupccl.
type SplitAndScatterer interface {
	split(
		ctx context.Context,
		splitKey roachpb.Key,
		expirationTime hlc.Timestamp,
	) error

	scatter(
		ctx context.Context,
		scatterKey roachpb.Key,
	) error

	now() hlc.Timestamp
}

type DBSplitAndScatter struct {
	DB *kv.DB
}

func (s *DBSplitAndScatter) split(
	ctx context.Context, splitKey roachpb.Key, expirationTime hlc.Timestamp,
) error {
	return s.DB.AdminSplit(ctx, splitKey, expirationTime)
}

func (s *DBSplitAndScatter) scatter(ctx context.Context, scatterKey roachpb.Key) error {
	_, pErr := kv.SendWrapped(ctx, s.DB.NonTransactionalSender(), &kvpb.AdminScatterRequest{
		RequestHeader: kvpb.RequestHeaderFromSpan(roachpb.Span{
			Key:    scatterKey,
			EndKey: scatterKey.Next(),
		}),
		RandomizeLeases: true,
		MaxSize:         1, // don't scatter non-empty ranges on resume.
	})
	return pErr.GoError()
}

func (s *DBSplitAndScatter) now() hlc.Timestamp {
	return s.DB.Clock().Now()
}

// CreateInitialSplits creates splits based on the given toplogy from the
// source. Parallelize splits by first sorting all the partition spans, and then
// sending an equal number of contiguous spans to split workers.
//
// The idea here is to use the information from the source cluster about
// the distribution of the data to produce split points to help prevent
// ingestion processors from pushing data into the same ranges during
// the initial scan.
func CreateInitialSplits(
	ctx context.Context,
	codec keys.SQLCodec,
	splitter SplitAndScatterer,
	sortedSpans roachpb.Spans,
	destNodeCount int,
	rekeyer *backupccl.KeyRewriter,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "crosscluster.createInitialSplits")
	defer sp.Finish()

	grp := ctxgroup.WithContext(ctx)
	splitWorkers := destNodeCount
	spansPerWorker := len(sortedSpans) / splitWorkers
	for i := 0; i < splitWorkers; i++ {
		startIdx := i * spansPerWorker
		endIdx := (i + 1) * spansPerWorker
		workerSpans := sortedSpans[startIdx:endIdx]
		if i == splitWorkers-1 {
			// The last worker handles the remainder spans
			workerSpans = sortedSpans[startIdx:]
		}
		grp.GoCtx(splitAndScatterWorker(workerSpans, rekeyer, splitter))
	}
	return grp.Wait()
}

// Spans are filled from left to right during the initial scan. Each might cover
// many (10-100+) source ranges merged to a single span by PartitionSpans, that
// could take multiple minutes to fill, so a span later in spans might be filled
// until minutes or hours later than one earlier in spans. Thus we want to give
// later splits longer enforcement times as we know that they may not be filled
// for longer, and we do not want them being merged away before we fill them.
const baseSplitExpiration = time.Hour * 6
const extraExpirationPerSpan = time.Minute * 10
const maxSplitExpiration = time.Hour * 24 * 7

func splitAndScatterWorker(
	spans []roachpb.Span, rekeyer *backupccl.KeyRewriter, splitter SplitAndScatterer,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for spanNum, span := range spans {
			startKey := span.Key.Clone()
			splitKey, _, err := rekeyer.RewriteTenant(startKey)
			if err != nil {
				return err
			}

			// NOTE(ssd): EnsureSafeSplitKey called on an arbitrary
			// key unfortunately results in many of our split keys
			// mapping to the same key for workloads like TPCC where
			// the schema of the table includes integers that will
			// get erroneously treated as the column family length.
			//
			// Since the partitions are generated from a call to
			// PartitionSpans on the source cluster, they should be
			// aligned with the split points in the original cluster
			// and thus should be valid split keys. But, we are
			// opening ourselves up to replicating bad splits from
			// the original cluster.
			//
			// if newSplitKey, err := keys.EnsureSafeSplitKey(splitKey); err != nil {
			// 	// Ignore the error since keys such as
			// 	// /Tenant/2/Table/13 is an OK start key but
			// 	// returns an error.
			// } else if len(newSplitKey) != 0 {
			// 	splitKey = newSplitKey
			// }
			//
			addedExpiration := time.Duration(spanNum) * extraExpirationPerSpan
			if err := splitAndScatter(ctx, splitKey, splitter, addedExpiration); err != nil {
				return err
			}

		}
		return nil
	}
}

func splitAndScatter(
	ctx context.Context, splitAndScatterKey roachpb.Key, s SplitAndScatterer, extra time.Duration,
) error {
	log.VInfof(ctx, 1, "splitting and scattering at %s", splitAndScatterKey)
	expirationTime := s.now().AddDuration(min(baseSplitExpiration+extra, maxSplitExpiration))
	if err := s.split(ctx, splitAndScatterKey, expirationTime); err != nil {
		return err
	}
	if err := s.scatter(ctx, splitAndScatterKey); err != nil {
		log.Warningf(ctx, "failed to scatter span starting at %s: %v",
			splitAndScatterKey, err)
	}
	return nil
}
