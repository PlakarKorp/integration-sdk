package integration

import "context"

// Provisioner creates a resource a snapshot can be restored into, and hands
// back the target an exporter restores to.
type Provisioner interface {
	Ping(ctx context.Context) error
	Provision(ctx context.Context, req ProvisionRequest) (*ProvisionResponse, error)
	Deprovision(ctx context.Context, handle string) error
	Close(ctx context.Context) error
}

// ProvisionRequest carries the labels to attach to what is created, as the
// provider allows: at least the job it was created for.
type ProvisionRequest struct {
	Labels map[string]string
}

// ProvisionResponse names what was created and the exporter target to
// restore into. Target may hold credentials: it is for the restore, not for
// storage.
type ProvisionResponse struct {
	Handle   string
	Protocol string
	Target   map[string]string
}
