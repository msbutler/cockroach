// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var ErrNodeLagging = errors.New("node frontier too far behind other nodes")

// checkLaggingNode returns an error if there exists a destination node lagging
// more than maxAllowable lag behind the mean frontier of all destination nodes. This function
// assumes that all nodes have finished their initial scan (i.e. have a nonzero hwm).
func checkLaggingNodes(
	ctx context.Context, ingestProcStats ingestStatsByNode, maxAllowableLag time.Duration,
) error {
	if maxAllowableLag == 0 {
		return nil
	}
	laggingNode, meanLagDifference := computeMeanLagDifference(ctx, ingestProcStats)
	log.VEventf(ctx, 2, "computed mean lag diff: %d lagging node, difference %.2f", laggingNode, meanLagDifference.Minutes())
	if maxAllowableLag < meanLagDifference {
		return errors.Wrapf(ErrNodeLagging, "node %d is %.2f minutes behind the average frontier. Try replanning", laggingNode, meanLagDifference.Minutes())
	}
	return nil
}

// computeMeanLagDifference computes the difference between the mean frontier by
// node and the node with the lowest frontier.
func computeMeanLagDifference(
	ctx context.Context, ingestProcsStats ingestStatsByNode,
) (base.SQLInstanceID, time.Duration) {

	if len(ingestProcsStats) < 2 {
		// If there are fewer than 2 nodes with updates, we can't compare relative lag.
		return base.SQLInstanceID(0), 0
	}

	lowestFrontier := hlc.MaxTimestamp.GoTime()
	meanFrontier := getMeanFrontier(ingestProcsStats)

	for node, stats := range ingestProcsStats {
		stats.Lock()
		defer stats.Unlock()
		if globalLowWaterMark.After(stats.LowWaterMark) {
			globalLowWaterMark = stats.LowWaterMark
			laggingNode = node
		}
	}
	log.VEventf(ctx, 2, "mean frontier: %s, lowest frontier %s", meanFrontier, lowestFrontier)

	return laggingNode, meanFrontier.Sub(lowestFrontier)
}

func getMeanFrontier(ingestProcStats ingestStatsByNode) time.Time {
	var sum int64
	for _, stat := range ingestProcStats {
		sum += stat.frontier.Unix()
	}
	frontierMean := timeutil.Unix(sum/int64(len(destNodeFrontier)), 0)
	return frontierMean
}
