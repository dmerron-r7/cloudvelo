package ingestion

import (
	"context"
	"strings"

	"www.velocidex.com/golang/cloudvelo/schema/api"
	"www.velocidex.com/golang/cloudvelo/services"
	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	crypto_proto "www.velocidex.com/golang/velociraptor/crypto/proto"
	flows_proto "www.velocidex.com/golang/velociraptor/flows/proto"
	velo_services "www.velocidex.com/golang/velociraptor/services"
	velo_utils "www.velocidex.com/golang/velociraptor/utils"
)

// When we receive a status we need to modify the collection record.
func (self Ingestor) HandleFlowStats(
	ctx context.Context,
	config_obj *config_proto.Config,
	message *crypto_proto.VeloMessage) error {

	if message == nil ||
		message.FlowStats == nil ||
		message.Source == "" ||
		message.SessionId == "" {
		return nil
	}

	msg := message.FlowStats
	query_stats := msg.QueryStatus

	// A client that does not know the flow can not complete it, so
	// the flow is recorded as failed and the hunt stats are left
	// alone.
	unknown_flow := isUnknownFlowReply(msg)
	if unknown_flow {
		running, err := isFlowRunning(ctx, config_obj,
			message.Source, message.SessionId)
		if err != nil || !running {
			return err
		}

		query_stats = []*crypto_proto.VeloStatus{{
			Status:       crypto_proto.VeloStatus_GENERIC_ERROR,
			ErrorMessage: msg.QueryStatus[0].GetErrorMessage(),
		}}
	}

	collector_context := &flows_proto.ArtifactCollectorContext{
		ClientId:                   message.Source,
		SessionId:                  message.SessionId,
		TotalUploadedFiles:         msg.TotalUploadedFiles,
		TotalExpectedUploadedBytes: msg.TotalExpectedUploadedBytes,
		TotalUploadedBytes:         msg.TotalUploadedBytes,
		TotalCollectedRows:         msg.TotalCollectedRows,
		TotalLogs:                  msg.TotalLogs,
		ActiveTime:                 msg.Timestamp,
		QueryStats:                 query_stats,
	}

	failed, completed := calcFlowOutcome(collector_context)

	// Progress messages are written to this doc id
	doc_id := api.GetDocumentIdForCollection(
		message.Source, message.SessionId, "stats")

	// Because we can not guarantee the order of messages written we
	// will write the final stat message to a different id. This
	// ensures it can not be overwritten with an incomplete status
	if completed {
		doc_id = api.GetDocumentIdForCollection(
			message.Source, message.SessionId, "completed")
	}

	stats := api.ArtifactCollectorRecordFromProto(collector_context, doc_id)
	stats.Type = "stats"

	// The status needs to hit the DB quickly, so the GUI can show
	// progress as the collection is received. The bulk data is still
	// stored asyncronously.
	err := services.SetElasticIndex(ctx,
		config_obj.OrgId,
		"transient", services.DocIdRandom,
		stats)
	if err != nil {
		return err
	}

	if unknown_flow {
		return nil
	}

	return self.maybeHandleHuntFlowStats(
		ctx, config_obj, collector_context, failed, completed)
}

// The client replies with a single UNKNOWN_FLOW status when asked
// about a flow it does not know. Older clients send a "Flow not
// known" error instead.
func isUnknownFlowReply(msg *crypto_proto.FlowStats) bool {
	if msg == nil || !msg.FlowComplete || len(msg.QueryStatus) != 1 {
		return false
	}

	status := msg.QueryStatus[0]
	return status.GetStatus() == crypto_proto.VeloStatus_UNKNOWN_FLOW ||
		strings.HasPrefix(status.GetErrorMessage(), "Flow not known")
}

// A flow that is missing or already finished must not be changed by
// a late reply.
func isFlowRunning(
	ctx context.Context,
	config_obj *config_proto.Config,
	client_id, flow_id string) (bool, error) {

	launcher, err := velo_services.GetLauncher(config_obj)
	if err != nil {
		return false, err
	}

	collection_context, err := launcher.Storage().LoadCollectionContext(
		ctx, config_obj, client_id, flow_id)
	if velo_utils.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	switch collection_context.State {
	case flows_proto.ArtifactCollectorContext_FINISHED,
		flows_proto.ArtifactCollectorContext_ERROR:
		return false, nil
	}

	return true, nil
}
