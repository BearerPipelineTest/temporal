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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type (
	ForceReplicationParams struct {
		Namespace               string
		Query                   string // query to list workflows for replication
		ConcurrentActivityCount int
		OverallRps              float64 // RPS for enqueuing of replication tasks
		ListWorkflowsPageSize   int     // PageSize of ListWorkflow, will paginate through results.
		PageCountPerExecution   int     // number of pages to be processed before continue as new, max is 1000.
		NextPageToken           []byte  // used by continue as new
	}

	listWorkflowsResponse struct {
		Executions    []commonpb.WorkflowExecution
		NextPageToken []byte
		Error         error
	}

	generateReplicationTasksRequest struct {
		NamespaceID string
		Executions  []commonpb.WorkflowExecution
		RPS         float64
	}

	metadataRequest struct {
		Namespace string
	}

	metadataResponse struct {
		ShardCount  int32
		NamespaceID string
	}
)

var (
	forceReplicationActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}
)

func ForceReplicationWorkflow(ctx workflow.Context, params ForceReplicationParams) error {
	if err := validateAndSetForceReplicationParams(&params); err != nil {
		return err
	}

	metadataResp, err := getClusterMetadata(ctx, params)
	if err != nil {
		return err
	}

	listWorkflowsCh := workflow.NewBufferedChannel(ctx, params.PageCountPerExecution)

	var continueAsNewPageToken []byte
	var listWorkflowsErr error
	workflow.Go(ctx, func(ctx workflow.Context) {
		continueAsNewPageToken, listWorkflowsErr = listWorkflowsForReplication(ctx, listWorkflowsCh, params)

		// enqueueReplicationTasks only returns when listWorkflowCh is closed (or if it encounters an error).
		// Therefore, continueAsNewPageToken and listWorkflowsErr is always set prior to their use.
		listWorkflowsCh.Close()
	})

	if err := enqueueReplicationTasks(ctx, listWorkflowsCh, metadataResp, params); err != nil {
		return err
	}

	if listWorkflowsErr != nil {
		return listWorkflowsErr
	}

	if continueAsNewPageToken == nil {
		return nil
	}

	// There are still more workflows to replicate. Continue-as-new to process on a new run.
	// This prevents history size from exceeding the server-defined limit
	params.NextPageToken = continueAsNewPageToken
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflow, params)
}

func validateAndSetForceReplicationParams(params *ForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}
	if params.OverallRps <= 0 {
		params.OverallRps = float64(params.ConcurrentActivityCount)
	}
	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultListWorkflowsPageSize
	}
	if params.PageCountPerExecution <= 0 {
		params.PageCountPerExecution = defaultPageCountPerExecution
	}
	if params.PageCountPerExecution > maxPageCountPerExecution {
		params.PageCountPerExecution = maxPageCountPerExecution
	}

	return nil
}

func getClusterMetadata(ctx workflow.Context, params ForceReplicationParams) (metadataResponse, error) {
	var a *activities

	// Get cluster metadata, we need namespace ID for history API call.
	// TODO: remove this step.
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithLocalActivityOptions(ctx, lao)
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteLocalActivity(actx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	return metadataResponse{}, err
}

func listWorkflowsForReplication(ctx workflow.Context, listWorkflowsCh workflow.Channel, params ForceReplicationParams) ([]byte, error) {
	var a *activities

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)

	for i := 0; i < params.PageCountPerExecution; i++ {
		listFuture := workflow.ExecuteActivity(actx, a.ListWorkflows, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     params.Namespace,
			PageSize:      int32(params.ListWorkflowsPageSize),
			NextPageToken: params.NextPageToken,
			Query:         params.Query,
		})

		var listResp listWorkflowsResponse
		if err := listFuture.Get(ctx, &listResp); err != nil {
			return nil, err
		}

		listWorkflowsCh.Send(ctx, listResp)

		params.NextPageToken = listResp.NextPageToken
		if params.NextPageToken == nil {
			return nil, nil
		}
	}

	return params.NextPageToken, nil
}

func enqueueReplicationTasks(ctx workflow.Context, listWorkflowsCh workflow.Channel, metadataResp metadataResponse, params ForceReplicationParams) error {
	selector := workflow.NewSelector(ctx)
	pendingActivities := 0

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	var futures []workflow.Future
	var listResp listWorkflowsResponse

	for listWorkflowsCh.Receive(ctx, &listResp) {
		replicationTaskFuture := workflow.ExecuteActivity(actx, a.GenerateReplicationTasks, &generateReplicationTasksRequest{
			NamespaceID: metadataResp.NamespaceID,
			Executions:  listResp.Executions,
			RPS:         params.OverallRps / float64(params.ConcurrentActivityCount),
		})

		pendingActivities++
		selector.AddFuture(replicationTaskFuture, func(f workflow.Future) {
			pendingActivities--
		})

		if pendingActivities == params.ConcurrentActivityCount {
			selector.Select(ctx) // this will block until one of the pending activities complete
		}

		futures = append(futures, replicationTaskFuture)
	}

	for _, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			return err
		}
	}

	return nil
}
