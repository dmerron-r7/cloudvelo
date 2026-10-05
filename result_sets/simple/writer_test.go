package simple

import (
	"context"
	"errors"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/stretchr/testify/assert"
	cvelo_services "www.velocidex.com/golang/cloudvelo/services"
	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	"www.velocidex.com/golang/velociraptor/file_store/path_specs"
	"www.velocidex.com/golang/velociraptor/json"
)

// fakeElastic records documents written by the writer and can be
// told to fail result set writes.
type fakeElastic struct {
	rows     []*SimpleResultSetRecord
	md       []ResultSetMetadataRecord
	rs_calls int
	fail_rs  bool
}

func (self *fakeElastic) setElasticIndex(ctx context.Context,
	org_id, index, id string, record interface{}) error {
	switch t := record.(type) {
	case *ResultSetMetadataRecord:
		self.md = append(self.md, *t)
	case *SimpleResultSetRecord:
		self.rs_calls++
		if self.rs_calls > 100 {
			panic("unbounded result set writes")
		}
		if self.fail_rs {
			return errors.New("injected elastic error")
		}
		self.rows = append(self.rows, t)
	}
	return nil
}

func (self *fakeElastic) setElasticIndexAsync(org_id, index, id string,
	action cvelo_services.BulkUpdateType, record interface{}) error {
	return self.setElasticIndex(context.Background(), org_id, index, id, record)
}

func (self *fakeElastic) lastMD() ResultSetMetadataRecord {
	if len(self.md) == 0 {
		return ResultSetMetadataRecord{}
	}
	return self.md[len(self.md)-1]
}

func installFakeElastic(t *testing.T) *fakeElastic {
	fake := &fakeElastic{}

	old_set, old_async, old_flush := setElasticIndex, setElasticIndexAsync, flushIndex
	setElasticIndex = fake.setElasticIndex
	setElasticIndexAsync = fake.setElasticIndexAsync
	flushIndex = func(ctx context.Context, org_id, index string) error {
		return nil
	}
	t.Cleanup(func() {
		setElasticIndex, setElasticIndexAsync, flushIndex = old_set, old_async, old_flush
	})

	return fake
}

func newTestWriter(sync bool) *ElasticSimpleResultSetWriter {
	return &ElasticSimpleResultSetWriter{
		log_path:   path_specs.NewSafeFilestorePath("clients", "C.1", "F.1"),
		opts:       json.DefaultEncOpts(),
		ctx:        context.Background(),
		config_obj: &config_proto.Config{},
		sync:       sync,
		md: &ResultSetMetadataRecord{
			ID:   "v1",
			Type: "rs_metadata",
		},
		version:             "v1",
		rows_per_result_set: 1000,
		max_size_per_packet: 1024 * 1024,
	}
}

func TestAbortEmptyBufferPersistsSentinel(t *testing.T) {
	fake := installFakeElastic(t)

	writer := newTestWriter(false)
	writer.Abort()

	assert.Equal(t, 0, len(fake.rows))
	assert.Equal(t, 1, len(fake.md))
	assert.Equal(t, int64(-1), fake.lastMD().TotalRows)
}

func TestAbortDiscardsBufferedRows(t *testing.T) {
	fake := installFakeElastic(t)

	writer := newTestWriter(false)
	writer.Write(ordereddict.NewDict().Set("A", 1))
	writer.Write(ordereddict.NewDict().Set("A", 2))
	writer.Abort()

	// Nothing written after the abort either.
	writer.Write(ordereddict.NewDict().Set("A", 3))
	writer.Close()

	assert.Equal(t, 0, len(fake.rows))
	assert.Equal(t, 1, len(fake.md))
	assert.Equal(t, int64(-1), fake.lastMD().TotalRows)
}

func TestPersistentWriteErrorTerminates(t *testing.T) {
	fake := installFakeElastic(t)
	fake.fail_rs = true

	// Mirrors client log ingestion: a sync writer with a small batch
	// that is only written by the deferred Close.
	writer := newTestWriter(true)
	writer.WriteJSONL([]byte("{\"A\":1}\n{\"A\":2}\n"), 2)
	writer.Close()
	writer.Close()

	assert.Equal(t, 1, fake.rs_calls)
	assert.Equal(t, 0, len(fake.rows))
	assert.Equal(t, 1, len(fake.md))
	assert.Equal(t, int64(-1), fake.lastMD().TotalRows)
	assert.Equal(t, int64(-1), fake.lastMD().EndRow)
}

func TestFlushAdvancesRows(t *testing.T) {
	fake := installFakeElastic(t)

	writer := newTestWriter(true)
	writer.WriteJSONL([]byte("{\"A\":1}\n{\"A\":2}\n"), 2)
	writer.Close()

	assert.Equal(t, 1, len(fake.rows))
	assert.Equal(t, int64(0), fake.rows[0].StartRow)
	assert.Equal(t, int64(2), fake.rows[0].EndRow)
	assert.Equal(t, int64(2), fake.lastMD().EndRow)
	assert.Equal(t, int64(0), fake.lastMD().TotalRows)
}
