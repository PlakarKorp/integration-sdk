package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/PlakarKorp/pkg"
)

type EndpointType string

const (
	EndpointUndefined  EndpointType = ""
	EndpointHost       EndpointType = "host"
	EndpointInet4      EndpointType = "inet4"
	EndpointInet6      EndpointType = "inet6"
	EndpointUUID       EndpointType = "uuid"
	EndpointNetwork    EndpointType = "network"
	EndpointIdentifier EndpointType = "identifier"
)

var (
	ErrInvalidEndpoint     = errors.New("invalid endpoint")
	ErrConnectorMismatch   = errors.New("cannot update connector type")
	ErrUnknownEndpointType = errors.New("unknown endpoint type")
	ErrDisabledSync        = errors.New("disabled sync")
)

type Inventory interface {
	List(context.Context, chan<- *InventoryEntry) error
	Close(context.Context) error
}

type HostEndpoint struct {
	Type       EndpointType
	Endpoint   string
	Attributes map[string]string
}
type InventoryEntry struct {
	Class    pkg.ResourceClass    // interpreted
	SubClass pkg.ResourceSubClass // interpreted
	Tags     []string             // <value>, <key>=<value>

	// The rest is provider data
	URN       string //
	Name      string //
	Region    string // eu-west-3, paris/dc-1, ...
	Service   string // s3, ec2, instance, dedicated-server, ...
	Resource  string // s3:bucket, ec2:volume, ...
	Details   []byte // Additional backend-specific content
	Endpoints []HostEndpoint

	// Fill via CountryOf. Report it on every entry you can.
	Country Country
}

func EndpointTypeFromString(t string) (EndpointType, error) {
	e := EndpointType(t)
	switch e {
	case EndpointHost:
	case EndpointInet4:
	case EndpointInet6:
	case EndpointIdentifier:
	default:
		return e, fmt.Errorf("%w: %s", ErrUnknownEndpointType, t)
	}

	return e, nil
}
