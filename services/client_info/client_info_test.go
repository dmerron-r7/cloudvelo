package client_info_test

import (
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert"
	"github.com/stretchr/testify/suite"
	"www.velocidex.com/golang/cloudvelo/schema/api"
	cvelo_services "www.velocidex.com/golang/cloudvelo/services"
	"www.velocidex.com/golang/cloudvelo/testsuite"
	actions_proto "www.velocidex.com/golang/velociraptor/actions/proto"
	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	"www.velocidex.com/golang/velociraptor/services"
	"www.velocidex.com/golang/velociraptor/utils"
	"www.velocidex.com/golang/velociraptor/vtesting"
)

type ClientInfoTestSuite struct {
	*testsuite.CloudTestSuite
}

// Labels and the last interrogate flow id live in their own documents in
// the index. ClientInfoManager.Get must surface both, because the labeler
// reads labels back through it.
func (self *ClientInfoTestSuite) TestGetReturnsLabelsAndInterrogateFlowId() {
	config_obj := self.ConfigObj.VeloConf()
	client_id := "C.1234labels"

	self.seedClient(config_obj, client_id)
	self.seedLabelRecord(config_obj, client_id, "Label1")
	self.seedInterrogateRecord(config_obj, client_id, "F.interrogate")

	self.waitForLabels(config_obj, client_id)

	client_info_manager, err := services.GetClientInfoManager(config_obj)
	assert.NoError(self.T(), err)

	client_info, err := client_info_manager.Get(self.Ctx, client_id)
	assert.NoError(self.T(), err)

	assert.Equal(self.T(), []string{"Label1"}, client_info.Labels)
	assert.Equal(self.T(), "F.interrogate", client_info.LastInterrogateFlowId)

	// The labeler reads through Get, so it must agree with it.
	labeler := services.GetLabeler(config_obj)
	assert.True(self.T(),
		labeler.IsLabelSet(self.Ctx, config_obj, client_id, "Label1"))
	assert.Equal(self.T(), []string{"Label1"},
		labeler.GetClientLabels(self.Ctx, config_obj, client_id))

	// A label that was never set must still report as unset.
	assert.False(self.T(),
		labeler.IsLabelSet(self.Ctx, config_obj, client_id, "NotSet"))
}

// Get reads the indexer's client record cache, which a direct index write
// does not touch. Writing a label must evict the entry, or the label stays
// invisible for the cache TTL.
func (self *ClientInfoTestSuite) TestLabelWriteInvalidatesClientCache() {
	config_obj := self.ConfigObj.VeloConf()
	client_id := "C.1234stale"

	self.seedClient(config_obj, client_id)

	client_info_manager, err := services.GetClientInfoManager(config_obj)
	assert.NoError(self.T(), err)

	// Prime the cache while the client still has no labels.
	var client_info *services.ClientInfo
	vtesting.WaitUntil(5*time.Second, self.T(), func() bool {
		client_info, err = client_info_manager.Get(self.Ctx, client_id)
		return err == nil
	})
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), 0, len(client_info.Labels))

	labeler := services.GetLabeler(config_obj)
	err = labeler.SetClientLabel(self.Ctx, config_obj, client_id, "Label1")
	assert.NoError(self.T(), err)

	// Confirm the write landed by reading around the cache, so that the
	// assertion below is about cache invalidation and not about how quickly
	// the index becomes consistent.
	self.waitForLabels(config_obj, client_id)

	client_info, err = client_info_manager.Get(self.Ctx, client_id)
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), []string{"Label1"}, client_info.Labels)

	assert.True(self.T(),
		labeler.IsLabelSet(self.Ctx, config_obj, client_id, "Label1"))

	// Removing it must become visible on the same terms.
	err = labeler.RemoveClientLabel(self.Ctx, config_obj, client_id, "Label1")
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	vtesting.WaitUntil(5*time.Second, self.T(), func() bool {
		records, err := api.GetMultipleClients(
			self.Ctx, config_obj, []string{client_id})
		return err == nil && len(records) == 1 && len(records[0].Labels) == 0
	})

	assert.False(self.T(),
		labeler.IsLabelSet(self.Ctx, config_obj, client_id, "Label1"))
}

func (self *ClientInfoTestSuite) seedClient(
	config_obj *config_proto.Config, client_id string) {

	client_info_manager, err := services.GetClientInfoManager(config_obj)
	assert.NoError(self.T(), err)

	err = client_info_manager.Set(self.Ctx, &services.ClientInfo{
		ClientInfo: &actions_proto.ClientInfo{
			ClientId: client_id,
			Hostname: "TestHost",
		}})
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)
}

// Mirrors the document the labeler writes when a client has no label
// record yet.
func (self *ClientInfoTestSuite) seedLabelRecord(
	config_obj *config_proto.Config, client_id, label string) {

	err := cvelo_services.SetElasticIndex(self.Ctx, config_obj.OrgId,
		"persisted", client_id+"_labels",
		&api.ClientRecord{
			ClientId:    client_id,
			Labels:      []string{label},
			LowerLabels: []string{strings.ToLower(label)},
			DocType:     "clients",
			Timestamp:   uint64(utils.GetTime().Now().Unix()),
		})
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)
}

// Mirrors Ingestor.HandleInterrogation.
func (self *ClientInfoTestSuite) seedInterrogateRecord(
	config_obj *config_proto.Config, client_id, flow_id string) {

	err := cvelo_services.SetElasticIndex(self.Ctx, config_obj.OrgId,
		"persisted", client_id+"_interrogate",
		&api.ClientRecord{
			ClientId:        client_id,
			Type:            "interrogation",
			LastInterrogate: flow_id,
			DocType:         "clients",
			Timestamp:       uint64(utils.GetTime().Now().Unix()),
		})
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)
}

// Read the client documents directly, bypassing the client record cache.
func (self *ClientInfoTestSuite) waitForLabels(
	config_obj *config_proto.Config, client_id string) {

	vtesting.WaitUntil(5*time.Second, self.T(), func() bool {
		records, err := api.GetMultipleClients(
			self.Ctx, config_obj, []string{client_id})
		return err == nil && len(records) == 1 && len(records[0].Labels) > 0
	})
}

func TestClientInfo(t *testing.T) {
	suite.Run(t, &ClientInfoTestSuite{
		CloudTestSuite: &testsuite.CloudTestSuite{
			Indexes: []string{"persisted"},
		},
	})
}
