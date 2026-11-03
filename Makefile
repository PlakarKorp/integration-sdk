GO =	go

all: rec

rec:
	${MAKE} -C proto

test: check

check:
	${GO} vet ./...
	${GO} test ./...

coverage:
	${GO} test -count=1 -coverprofile=coverage.out -covermode=atomic ./...
	grep -v -E '\.pb\.go:' coverage.out > coverage.out.tmp && mv coverage.out.tmp coverage.out
	${GO} tool cover -func=coverage.out | tail -1

.PHONY: all rec test check coverage
