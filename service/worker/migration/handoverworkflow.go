// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package migration

import (
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

const (
	forceReplicationWorkflowName  = "force-replication"
	namespaceHandoverWorkflowName = "namespace-handover"

	defaultListWorkflowsPageSize = 1000
	defaultPageCountPerExecution = 200
	maxPageCountPerExecution     = 1000

	minimumAllowedLaggingSeconds  = 5
	minimumHandoverTimeoutSeconds = 30
)

type (
	NamespaceHandoverParams struct {
		Namespace     string
		RemoteCluster string

		// how far behind on replication is allowed for remote cluster before handover is initiated
		AllowedLaggingSeconds int
		// how long to wait for handover to complete before rollback
		HandoverTimeoutSeconds int
	}

	activities struct {
		historyShardCount int32
		executionManager  persistence.ExecutionManager
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient
		frontendClient    workflowservice.WorkflowServiceClient
		logger            log.Logger
		metricsClient     metrics.Client
	}

	replicationStatus struct {
		MaxReplicationTaskIds map[int32]int64 // max replication task id for each shard.
	}

	waitReplicationRequest struct {
		ShardCount     int32
		RemoteCluster  string          // remote cluster name
		WaitForTaskIds map[int32]int64 // remote acked replication task needs to pass this id
		AllowedLagging time.Duration   // allowed remote acked lagging
	}

	updateStateRequest struct {
		Namespace string // move this namespace into Handover state
		NewState  enumspb.ReplicationState
	}

	updateActiveClusterRequest struct {
		Namespace     string // move this namespace into Handover state
		ActiveCluster string
	}

	waitHandoverRequest struct {
		ShardCount    int32
		Namespace     string
		RemoteCluster string // remote cluster name
	}
)

var (
	historyServiceRetryPolicy = common.CreateHistoryServiceRetryPolicy()
)

func NamespaceHandoverWorkflow(ctx workflow.Context, params NamespaceHandoverParams) error {
	// validate input params
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if len(params.RemoteCluster) == 0 {
		return errors.New("InvalidArgument: RemoteCluster is required")
	}
	if params.AllowedLaggingSeconds <= minimumAllowedLaggingSeconds {
		params.AllowedLaggingSeconds = minimumAllowedLaggingSeconds
	}
	if params.HandoverTimeoutSeconds <= minimumHandoverTimeoutSeconds {
		params.HandoverTimeoutSeconds = minimumHandoverTimeoutSeconds
	}

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		MaximumInterval:    time.Second,
		BackoffCoefficient: 1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var a *activities

	// ** Step 1, Get cluster metadata **
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteActivity(ctx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	if err != nil {
		return err
	}

	// ** Step 2, get current replication status **
	var repStatus replicationStatus
	err = workflow.ExecuteActivity(ctx, a.GetMaxReplicationTaskIDs).Get(ctx, &repStatus)
	if err != nil {
		return err
	}

	// ** Step 3, wait remote cluster to catch up on replication tasks
	ao3 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx3 := workflow.WithActivityOptions(ctx, ao3)
	waitRequest := waitReplicationRequest{
		ShardCount:     metadataResp.ShardCount,
		RemoteCluster:  params.RemoteCluster,
		AllowedLagging: time.Duration(params.AllowedLaggingSeconds) * time.Second,
		WaitForTaskIds: repStatus.MaxReplicationTaskIds,
	}
	err = workflow.ExecuteActivity(ctx3, a.WaitReplication, waitRequest).Get(ctx3, nil)
	if err != nil {
		return err
	}

	// ** Step 4, initiate handover
	handoverRequest := updateStateRequest{
		Namespace: params.Namespace,
		NewState:  enumspb.REPLICATION_STATE_HANDOVER,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, handoverRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	// ** Step 5, wait remote to ack handover task id
	ao5 := workflow.ActivityOptions{
		StartToCloseTimeout:    time.Second * 30,
		HeartbeatTimeout:       time.Second * 10,
		ScheduleToCloseTimeout: time.Second * time.Duration(params.HandoverTimeoutSeconds),
		RetryPolicy:            retryPolicy,
	}
	ctx5 := workflow.WithActivityOptions(ctx, ao5)
	waitHandover := waitHandoverRequest{
		ShardCount:    metadataResp.ShardCount,
		Namespace:     params.Namespace,
		RemoteCluster: params.RemoteCluster,
	}
	err5 := workflow.ExecuteActivity(ctx5, a.WaitHandover, waitHandover).Get(ctx5, nil)
	if err5 == nil {
		// ** Step 6, remote cluster is ready to take over, update Namespace to use remote cluster as active
		updateRequest := updateActiveClusterRequest{
			Namespace:     params.Namespace,
			ActiveCluster: params.RemoteCluster,
		}
		err = workflow.ExecuteActivity(ctx, a.UpdateActiveCluster, updateRequest).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// ** Step 7, reset namespace state from Handover -> Registered
	resetStateRequest := updateStateRequest{
		Namespace: params.Namespace,
		NewState:  enumspb.REPLICATION_STATE_NORMAL,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, resetStateRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	return err5
}
