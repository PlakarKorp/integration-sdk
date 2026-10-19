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

.PHONY: all build gen

include Makefile.inc
