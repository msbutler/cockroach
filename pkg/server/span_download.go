// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (t *statusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = t.AnnotateCtx(ctx)

	// TODO(adityamaru): Figure out proper privileges, for now we restrict this
	// call to system tenant only in the tenant connector.
	return t.sqlServer.tenantConnect.DownloadSpan(ctx, req)
}

func (s *systemStatusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	resp := &serverpb.DownloadSpanResponse{
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}
	if len(req.NodeID) > 0 {
		_, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		if local {
			return resp, s.localDownloadSpan(ctx, req)
		}

		return nil, errors.AssertionFailedf("requesting download on a specific node is not supported yet")
	}

	// Send DownloadSpan request to all stores on all nodes.
	remoteRequest := serverpb.DownloadSpanRequest{NodeID: "local", Spans: req.Spans}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.DownloadSpanResponse, error) {
		return status.DownloadSpan(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, downloadSpanResp *serverpb.DownloadSpanResponse) {}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		resp.ErrorsByNodeID[nodeID] = err.Error()
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "download spans",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return resp, nil
}

func (s *systemStatusServer) localDownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) error {

	return s.stores.VisitStores(func(store *kvserver.Store) error {
		spanCh := make(chan roachpb.Span)

		grp := ctxgroup.WithContext(ctx)
		grp.GoCtx(func(ctx context.Context) error {
			defer close(spanCh)
			ctxDone := ctx.Done()

			ids := make([]roachpb.RangeID, 0, store.ReplicaCount())
			for _, reqSpan := range req.Spans {
				if err := store.VisitReplicasByKey(ctx,
					roachpb.RKey(reqSpan.Key), roachpb.RKey(reqSpan.EndKey), kvserver.AscendingKeyOrder,
					func(ctx context.Context, r *kvserver.Replica) error {
						ids = append(ids, r.RangeID)
						return nil
					},
				); err != nil {
					return err
				}

				var downloadSpan roachpb.Span
				for _, id := range ids {
					r := store.GetReplicaIfExists(id)
					if r == nil {
						continue
					}
					if !r.OwnsValidLease(ctx, s.clock.NowAsClockTimestamp()) {
						continue
					}
					sp := r.Desc().KeySpan().AsRawSpanWithNoLocals().Intersect(reqSpan)

					if downloadSpan.EndKey.Equal(sp.Key) {
						downloadSpan.EndKey = sp.EndKey
						continue
					}
					if len(downloadSpan.Key) > 0 {
						spanCh <- downloadSpan
					}
					downloadSpan = sp
				}
				if len(downloadSpan.Key) > 0 {
					select {
					case spanCh <- downloadSpan:
					case <-ctxDone:
						return ctx.Err()
					}
				}
			}

			for _, sp := range req.Spans {
				select {
				case spanCh <- sp:
				case <-ctxDone:
					return ctx.Err()
				}
			}
			return nil
		})

		downloader := func(ctx context.Context) error {
			for sp := range spanCh {
				if err := store.TODOEngine().Download(ctx, sp); err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i < 4; i++ {
			grp.GoCtx(downloader)
		}
		return grp.Wait()
	})
}
