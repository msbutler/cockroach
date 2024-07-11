// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// TODO(azhu): create table and enum in the db to be replicated into instead of defaultdb.
	dlqBaseTableName = "defaultdb.crdb_replication_conflict_dlq_%d"
	writeEnumStmt    = `CREATE TYPE IF NOT EXISTS defaultdb.crdb_replication_mutation_type AS ENUM (
			'insert', 'update', 'delete'
	)`

	createTableStmt = `CREATE TABLE IF NOT EXISTS %s (
			id                  INT8 DEFAULT unique_rowid(),
			ingestion_job_id    INT8 NOT NULL,
  		table_id    				INT8 NOT NULL,
			dlq_timestamp     	TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
  		dlq_reason					STRING NOT NULL,
			mutation_type				defaultdb.crdb_replication_mutation_type,       
  		key_value_bytes			BYTES NOT NULL,
			incoming_row     		JSONB NULL,
  		-- PK should be unique based on the ID, job ID and timestamp at which the 
  		-- row was written to the table.
  		-- For any table being replicated in an LDR job, there should not be rows 
  		-- where they have identical ID and were written to the table at the same 
  		-- time.
			PRIMARY KEY (ingestion_job_id, dlq_timestamp, id) USING HASH
		)`

	insertRowStmtNotRow = `INSERT INTO %s (
			ingestion_job_id, 
			table_id, 
			dlq_reason,
			mutation_type,
			key_value_bytes,
		) VALUES ($1, $2, $3, $4, $5`
	firstJSONPlaceholder = 6
)

type ReplicationMutationType int

const (
	Insert ReplicationMutationType = iota
	Delete
	Update
)

func (t ReplicationMutationType) String() string {
	switch t {
	case Insert:
		return "insert"
	case Delete:
		return "delete"
	case Update:
		return "update"
	default:
		return fmt.Sprintf("Unrecognized ReplicationMutationType(%d)", int(t))
	}
}

type DeadLetterQueueClient interface {
	Create(ctx context.Context, tableIDs []int32) error

	Log(
		ctx context.Context,
		ingestionJobID int64,
		kv streampb.StreamEvent_KV,
		cdcEventRow cdcevent.Row,
		dlqReason retryEligibility,
	) error
}

type loggingDeadLetterQueueClient struct {
}

func (dlq *loggingDeadLetterQueueClient) Create(ctx context.Context, tableIDs []int32) error {
	return nil
}

func (dlq *loggingDeadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	dlqReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	var mutationType ReplicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = Delete
	} else {
		mutationType = Insert
	}

	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	log.Infof(ctx, `ingestion_job_id: %d,  
		table_id: %d, 
		dlq_reason: %s, 
		mutation_type: %s,  
		key_value_bytes: %v, 
		incoming_row: %s`,
		ingestionJobID, tableID, dlqReason.String(), mutationType.String(), bytes, cdcEventRow.DebugString())
	return nil
}

type deadLetterQueueClient struct {
	ie isql.Executor
}

func (dlq *deadLetterQueueClient) Create(ctx context.Context, tableIDs []int32) error {
	if _, err := dlq.ie.Exec(ctx, "create-enum", nil, writeEnumStmt); err != nil {
		return errors.Wrap(err, "failed to create crdb_replication_mutation_type enum")
	}

	// Create a dlq table for each given table.
	for _, tableID := range tableIDs {
		tableName := fmt.Sprintf(dlqBaseTableName, tableID)
		if _, err := dlq.ie.Exec(ctx, "create-dlq-table", nil, fmt.Sprintf(createTableStmt, tableName)); err != nil {
			return errors.Wrapf(err, "failed to create dlq table %q", tableName)
		}
	}
	return nil
}

func addJSONTOInsert(
	insertStatementBase string, cdcEventRow cdcevent.Row,
) (string, []interface{}, error) {
	var fullStmt strings.Builder
	scatchDatums := make([]interface{}, len(cdcEventRow.ResultColumns()))

	fullStmt.WriteString(insertStatementBase)

	placeHolder := firstJSONPlaceholder
	// fails on tables with non visible columns and with column families
	iter := cdcEventRow.ForEachColumn()

	if err := iter.Datum(func(datum tree.Datum, col cdcevent.ResultColumn) error {
		fullStmt.WriteString(fmt.Sprintf(", $%d", placeHolder))
		placeHolder++
		scatchDatums = append(scatchDatums, datum)
		return nil
	}); err != nil {
		return "", nil, err
	}
	fullStmt.WriteString(")")
	return fullStmt.String(), scatchDatums, nil
}

func (dlq *deadLetterQueueClient) Log(
	ctx context.Context,
	ingestionJobID int64,
	kv streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	dlqReason retryEligibility,
) error {
	if !cdcEventRow.IsInitialized() {
		return errors.New("cdc event row not initialized")
	}

	tableID := cdcEventRow.TableID
	tableName := fmt.Sprintf(dlqBaseTableName, tableID)
	bytes, err := protoutil.Marshal(&kv)
	if err != nil {
		return errors.Wrap(err, "failed to marshal kv event")
	}

	// TODO(azhu): include update type
	var mutationType ReplicationMutationType
	if cdcEventRow.IsDeleted() {
		mutationType = Delete
	} else {
		mutationType = Insert
	}

	// Attempt to insert row into DLQ with a json'd row, and if that fails, just insert the bytes.
	insertStmtWithJSON, datums, err := addJSONTOInsert(fmt.Sprintf(insertRowStmtNotRow, tableName), cdcEventRow)
	if err != nil {
		return errors.Wrap(err, "failed to get datums for dlq insert with json'd row")
	}

	queryWithJsonArgs := append([]interface{}{ingestionJobID, tableID, dlqReason.String(), mutationType.String(), bytes}, datums...)

	if _, err := dlq.ie.Exec(
		ctx,
		"insert-row-into-dlq-table-with-json",
		nil, /* txn */
		fmt.Sprintf(insertStmtWithJSON, tableName),
		queryWithJsonArgs...,
	); err != nil {
		log.Warningf(ctx, "failed to insert row for table %s with json'd row: %v", tableName, err)
		insertRowFailback := fmt.Sprintf(insertRowStmtNotRow+")", tableName)
		if _, err := dlq.ie.Exec(
			ctx,
			"insert-row-into-dlq-table",
			nil, /* txn */
			insertRowFailback,
			ingestionJobID,
			tableID,
			dlqReason.String(),
			mutationType.String(),
			bytes,
		); err != nil {
			return errors.Wrapf(err, "failed to insert row for table %s", tableName)
		}
	}
	return nil
}

func InitDeadLetterQueueClient(ie isql.Executor) DeadLetterQueueClient {
	return &deadLetterQueueClient{ie: ie}
}

func InitLoggingDeadLetterQueueClient() DeadLetterQueueClient {
	return &loggingDeadLetterQueueClient{}
}
