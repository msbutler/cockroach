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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func registerMultiTenantSharedProcess(r registry.Registry) {
	crdbNodeCount := 4

	r.Add(registry.TestSpec{
		Name:    "multitenant/shared-process/basic",
		Owner:   registry.OwnerMultiTenant,
		Cluster: r.MakeClusterSpec(crdbNodeCount + 1),
		Timeout: 1 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			var (
				appTenantName  = "app"
				tpccWarehouses = 500
				crdbNodes      = c.Range(1, crdbNodeCount)
				workloadNode   = c.Node(crdbNodeCount + 1)
			)
			t.Status(`set up Unified Architecture Cluster`)
			c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

			// In order to observe the app tenant's db console, create a secure
			// cluster and add Admin roles to the system and app tenant.
			clusterSettings := install.MakeClusterSettings(install.SecureOption(true))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), clusterSettings, crdbNodes)

			sysConn := c.Conn(ctx, t.L(), crdbNodes.RandNode()[0])
			sysSQL := sqlutils.MakeSQLRunner(sysConn)

			createTenantAdminRole(t, "system", sysSQL)

			createInMemoryTenant(ctx, t, c, appTenantName, crdbNodes, true)

			dt, err := roachtestutil.NewDiskUsageTracker(c, t.L())
			require.NoError(t, err)

			printThroughput := func(initTime time.Time, initDu int) {
				postTime := timeutil.Now()
				postDU := dt.GetDiskUsage(ctx, crdbNodes)
				t.L().Printf(`init completed in %.2f minutes,and ingested %d mb of data. agg throughput %.2f MB/Min`,
					postTime.Sub(initTime).Minutes(),
					postDU-initDu,
					float64(postDU-initDu)/postTime.Sub(initTime).Minutes())
			}

			t.Status(`initialize tpcc workload`)
			initTime := timeutil.Now()
			initDU := dt.GetDiskUsage(ctx, crdbNodes)
			initCmd := fmt.Sprintf(`./workload init tpcc --data-loader import --warehouses %d {pgurl%s:%s}`,
				tpccWarehouses, crdbNodes, appTenantName)
			c.Run(ctx, workloadNode, initCmd)
			printThroughput(initTime, initDU)

			t.Status(`run tpcc workload`)
			postInitTime := timeutil.Now()
			postInitDU := dt.GetDiskUsage(ctx, crdbNodes)
			runCmd := fmt.Sprintf(`./workload run tpcc --warehouses %d --tolerate-errors --duration 10m {pgurl%s:%s}`,
				tpccWarehouses, crdbNodes, appTenantName)
			c.Run(ctx, workloadNode, runCmd)
			printThroughput(postInitTime, postInitDU)
		},
	})

}
