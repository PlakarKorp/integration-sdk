# Integration SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/PlakarKorp/integration-sdk.svg)](https://pkg.go.dev/github.com/PlakarKorp/integration-sdk)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)

Here you'll find the SDKs for all [Plakar][plakar] integrations.

[plakar]: https://github.com/PlakarKorp/plakar.git


Core concepts:

 * every Plakar integration is a standalone binary;
 * communications happens via GRPC;
 * Plakar acts as a client, and the integration as a Server;
 * integrations can be written in any language in theory, in practice
   only the Go binding are officially provided.


The layout is as follows:

 - the root contains just the integrations interfaces and ancillary types

 - `proto/` contains the protobuf definition and the generated
   Go code.  If you intend to write integrations in other language,
   you can start generating the *server* component for your target
   language using these.

 - `server/` contains the machinery for an integration to talk with
   plakar.  All integrations should import this package and use either
   the `Entrypoint*()` or `Run*On()` functions family.

 - `client/` contains the machinery to talk with an integration, i.e.
   the plakar side.  Development-aid tools also will use this.

 - `cmd/` contains some helpers to run integrations for development
   purposes.

 - `example/` contains an example inventory.
