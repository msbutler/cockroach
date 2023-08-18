// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package replicationtestutils

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/stretchr/testify/require"
)

// MakeSpanConfigRecord makes a dummy span config record with the given ttl. If the ttl is 0,  no span config is added,
// and the record is treated as a delete record.
func MakeSpanConfigRecord(t *testing.T, targetSpan roachpb.Span, ttl int) spanconfig.Record {
	target := spanconfig.MakeTargetFromSpan(targetSpan)
	var spanConfig roachpb.SpanConfig
	if ttl > 0 {
		spanConfig = roachpb.SpanConfig{
			GCPolicy: roachpb.GCPolicy{
				TTLSeconds: int32(ttl),
			},
		}
	}
	// check that all orderedUpdates are observed.
	record, err := spanconfig.MakeRecord(target, spanConfig)
	require.NoError(t, err)
	return record
}

func RecordToEntry(record spanconfig.Record) roachpb.SpanConfigEntry {
	t := record.GetTarget().ToProto()
	c := record.GetConfig()
	return roachpb.SpanConfigEntry{
		Target: t,
		Config: c,
	}
}

func CreateReplicationHelperWithDummySpanConfigTable(t *testing.T) (*ReplicationHelper, *tree.TableName, *spanconfigkvaccessor.KVAccessor, func()) {
	// Use a dummy span config table to avoid dealing with the default span configs set on the tenant.
	const dummySpanConfigurationsName = "dummy_span_configurations"
	dummyFQN := tree.NewTableNameWithSchema("d", catconstants.PublicSchemaName, dummySpanConfigurationsName)

	h, cleanup := NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Streaming: &sql.StreamingTestingKnobs{
				MockSpanConfigTableName: dummyFQN,
			},
		},
	})

	h.SysSQL.Exec(t, `
CREATE DATABASE d;
USE d;`)
	h.SysSQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyFQN))

	accessor := spanconfigkvaccessor.New(
		h.SysServer.DB(),
		h.SysServer.InternalExecutor().(isql.Executor),
		h.SysServer.ClusterSettings(),
		h.SysServer.Clock(),
		dummyFQN.String(),
		nil, /* knobs */
	)

	return h, dummyFQN, accessor, cleanup
}
