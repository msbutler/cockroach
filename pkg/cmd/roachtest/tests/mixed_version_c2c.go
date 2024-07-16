// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerC2CMixedVersions(r registry.Registry) {

	sp := replicationSpec{
		srcNodes: 4,
		dstNodes: 4,
		// The timeout field ensures the c2c roachtest driver behaves properly.
		timeout:                   10 * time.Minute,
		workload:                  replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute, maxBlockBytes: 1, initWithSplitAndScatter: true},
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
		suites:                    registry.Suites(registry.Nightly),
	}

	r.Add(registry.TestSpec{
		Name:             "c2c/mixed-versions",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(sp.dstNodes + sp.srcNodes + 1),
		CompatibleClouds: sp.clouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runC2CMixedVersions(ctx, t, c, sp)
		},
	})
}

var (
	expectedMajorUpgrades = 1
	minSupportedVersion   = "v23.2.0"
	sourceTenantName      = "source"
	destTenantName        = "destination"
)

// TODO (msbutler): schedule upgrades during initial scan and cutover.
func runC2CMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster, sp replicationSpec) {

	sourceMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(1, sp.srcNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.DisableSkipVersionUpgrades,
	)

	destMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(sp.srcNodes+1, sp.srcNodes+sp.dstNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.DisableSkipVersionUpgrades,
	)

	rd := makeReplicationDriver(t, c, sp)
	cleanup := rd.setupC2C(ctx, t, c)
	defer cleanup()

	var pgUrl chan *url.URL

	sourceMvt.OnStartup("source: create app tenant", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		srcNode := c.Node(1)
		srcClusterSetting := install.MakeClusterSettings()
		addr, err := c.ExternalPGUrl(ctx, t.L(), srcNode, roachprod.PGURLOptions{})
		if err != nil {
			return err
		}
		deprecatedStartInMemoryTenant(ctx, t, c, sourceTenantName, c.Range(1, sp.srcNodes))

		pgURL, err := copyPGCertsAndMakeURL(ctx, t, c, srcNode, srcClusterSetting.PGUrlCertsDir, addr[0])
		if err != nil {
			return err
		}
		pgUrl <- pgURL
		return nil
	})

	destMvt.OnStartup("dest: start replication", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		sourcePGURL := <-pgUrl
		h.System.Exec(r, fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'", destTenantName, sourceTenantName, sourcePGURL.String()))
		return nil
	})

	tpccInitCmd := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s:%s}", c.Range(1, sp.srcNodes), sourceTenantName)
	tpccRunCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s:%s}", c.Range(1, sp.srcNodes), sourceTenantName).
		Option("tolerate-errors").
		Flag("warehouses", 100)
	stopWorkload := sourceMvt.Workload("tpcc", c.Range(1, sp.srcNodes), tpccInitCmd, tpccRunCmd)

	// Ensure the source always waits to finalize until after the dest finalizes.
	destFinalized := make(chan struct{}, 1)

	// For a given major version update this can be called three times: upgrade, downgrade, upgrade again
	sourceMvt.InMixedVersion("wait for dest to finalize", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		if h.Context().Stage == mixedversion.LastUpgradeStage {
			l.Printf("waiting for destination cluster to finalize upgrade")
			<-destFinalized
		}

		return nil
	})

	// Called at the end of each major version upgrade.
	majorUpgradeCount := 0
	destMvt.AfterUpgradeFinalized("cutover and allow source to finalize", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		destFinalized <- struct{}{}
		majorUpgradeCount++
		if majorUpgradeCount == expectedMajorUpgrades {
			return afterLastMajorUpgrade(ctx, l, r, h)
		}

		// run dest fingerprint
		return nil
	})

	sourceMvt.AfterUpgradeFinalized("fingerprint source", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		stopWorkload()

		//run source fingerprint
		return nil
	})

	sourceMvt.Run()
	destMvt.Run()

	// Compare fingerprints
}

func afterLastMajorUpgrade(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
	err := h.System.Exec(r, "ALTER TENANT $1 COMPLETE REPLICATION TO LATEST", destTenantName)
	if err != nil {
		return err
	}
	return nil
}
