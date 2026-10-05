package notebook

import (
	"context"
	"sync"

	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	"www.velocidex.com/golang/velociraptor/services"
	"www.velocidex.com/golang/velociraptor/services/notebook"
)

// The NotebookManager service is the main entry point to the
// notebooks. It is composed by various storage related
// implementations which can be locally overriden for cloud
// environments.
type NotebookManager struct {
	*notebook.NotebookManager
	config_obj *config_proto.Config
}

func NewNotebookManagerService(
	ctx context.Context,
	wg *sync.WaitGroup,
	config_obj *config_proto.Config) services.NotebookManager {

	timeline_storer := NewSuperTimelineStorer(config_obj)
	store := NewNotebookStore(ctx, wg, config_obj, timeline_storer)

	annotator := NewSuperTimelineAnnotator(config_obj, timeline_storer)

	notebook_service := notebook.NewNotebookManager(config_obj, store,
		timeline_storer, &SuperTimelineReader{}, &SuperTimelineWriter{},
		annotator, notebook.NewAttachmentManager(config_obj, store))

	// Upstream's own constructor registers a NotebookBackupProvider here
	// so notebooks land in the server backup archive. That archive is
	// produced by the BackupService, which LazyServiceContainer does not
	// implement (services/orgs/lazy.go:65) because cloud notebooks are
	// already durable in Elastic and S3. Registering a provider with no
	// service to collect it would only add a dead error path.
	//
	// Calling NewNotebookManager rather than upstream's
	// NewNotebookManagerService also skips the Start() that upstream
	// returns alongside the service, so its root-org guard and
	// NotebookNumberOfLocalWorkers handling never run; the pool is
	// started instead in startup/gui.go. Anything upstream adds to
	// Start() will not take effect here. Only the backup omission above
	// has been reviewed and decided.
	return notebook_service
}
