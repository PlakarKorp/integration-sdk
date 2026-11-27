PROTO = connectors
PKGNAME =

all: gen build

build:
	go build -v ./...

gen: connectors.pb.go
	${MAKE} -C exporter
	${MAKE} -C importer
	${MAKE} -C storage
	${MAKE} -C inventory
	${MAKE} -C secret
	${MAKE} -C resolver
	${MAKE} -C provisioner

.PHONY: all build gen

include Makefile.inc
