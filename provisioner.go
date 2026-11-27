package integration

import "context"

// Provisioner creates a resource a snapshot is restored into. It works in two
// steps around the restore, because there are two kinds of resource:
//
//   - a container that restore fills: a directory, a bucket, an empty
//     volume.  Provision creates it and Finalize has nothing left to
//     do;
//   - a resource built from the restored data: e.g. an instance that
//     boots from a disk got from the snapshot.  Provision only says
//     where the exporter puts the data, and Finalize builds the
//     resource from it.
//
// Provision answers a handle before anything is restored, so whatever a
// failed or abandoned restore leaves behind can be deprovisioned.
type Provisioner interface {
	Ping(context.Context) error

	// Provision runs before the restore and answers the exporter target.
	Provision(context.Context, ProvisionRequest) (*ProvisionResponse, error)

	// Finalize runs once the restore into the target succeeded.
	Finalize(context.Context, FinalizeRequest) (*FinalizeResponse, error)

	// Deprovision deletes everything the handle names, at whatever step the
	// restore stopped. It is safe to call twice.
	Deprovision(context.Context, string) error

	Close(context.Context) error
}

// ProvisionRequest describes what to create.  Params are what this
// one resource is made of, validated against the provisioner's
// parameters schema: the configuration the provisioner was started
// with is only who creates it.  Labels are attached to what is
// created, as the provider allows: at least the job it was created
// for.
type ProvisionRequest struct {
	Labels map[string]string
	Params map[string]string
}

// FinalizeRequest is what Provision was given, with the handle it answered.
type FinalizeRequest struct {
	Handle string
	Labels map[string]string
	Params map[string]string
}

// FinalizeResponse says what now exists.  An empty Handle keeps the
// one Provision answered; an empty Location keeps the target's.
type FinalizeResponse struct {
	Handle   string
	Location string
}

// ProvisionResponse names what was created and the exporter target to
// restore into.  Target may hold credentials: it is for the restore,
// not for storage.
type ProvisionResponse struct {
	Handle   string
	Protocol string
	Target   map[string]string
}
