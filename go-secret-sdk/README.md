# Secret Provider SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/PlakarKorp/go-secret-sdk.svg)](https://pkg.go.dev/github.com/PlakarKorp/go-secret-sdk)
[![Go Report Card](https://goreportcard.com/badge/github.com/PlakarKorp/go-secret-sdk)](https://goreportcard.com/report/github.com/PlakarKorp/go-secret-sdk)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)

This repo contains the SDK for Plakar Control-Plane secrets providers.
Secret Providers are plugins that resolve secrets on the behalf of
Plakar Control-Plane at runtime, so we don't have to know the secrets.

This repository contains both the plugin and the consumer part.  The
plugin (the "server") part is what needs to be used by inventory
plugins, while the consumer ("client") is to receive the information
provided from an inventory plugin.

To ease the development, use `cmd/secrets` to test your inventory:

	$ make
	$ ./secrets -o param1=foo -o param2=bar ./path/to/secret-provider-executable key

this should print the resolved secret for the given `key`.


## Example Provider

The [rot13](./cmd/rot13) dummy secret provider can be used for
scaffolding purposes.

To try it out:

	$ make
	$ ./secrets ./rot13 bar

