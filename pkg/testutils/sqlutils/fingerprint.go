// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func FingerprintTable(t testing.TB, sqlDB *SQLRunner, tableID uint32) int {
	fingerprintQuery := fmt.Sprintf(`
SELECT *
FROM
	crdb_internal.fingerprint(
		crdb_internal.table_span(%d),
		true
	)`, tableID)

	var fingerprint int
	sqlDB.QueryRow(t, fingerprintQuery).Scan(&fingerprint)
	return fingerprint
}

func ClunkyFingerprintTable(t testing.TB, conn *gosql.DB, tableName string) int {
	/*oldNow := timeutil.Now()
	printReg, err := fingerprint(ctx, conn, "tpce", tableName)
	require.NoError(t, err)
	fmt.Printf("old method took %.2f minutes", timeutil.Since(oldNow).Minutes())
	fmt.Printf("print reg: %s\n", printReg)*/

	newNow := timeutil.Now()
	startTime := newNow.Add(-time.Hour)
	microSecondRFC3339Format := "2006-01-02 15:04:05.999999"
	startTimeStr := startTime.Format(microSecondRFC3339Format)
	skipTSfingerprintQuery := fmt.Sprintf(`
SELECT *
FROM 
  crdb_internal.fingerprint(
    crdb_internal.table_span((SELECT id FROM system.namespace WHERE name = '%s' AND "parentID" != 0)::INT),
    '%s'::TIMESTAMPTZ, 
    true
)`, tableName, startTimeStr)
	var newFingerprint int
	require.NoError(t, conn.QueryRow(skipTSfingerprintQuery).Scan(&newFingerprint))
	fmt.Printf("new method took %.2f minutes", timeutil.Since(newNow).Minutes())
	fmt.Printf("print reg: %d\n", newFingerprint)
	return newFingerprint
}
