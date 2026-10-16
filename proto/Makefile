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

.PHONY: all build gen

include Makefile.inc
