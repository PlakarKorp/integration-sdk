GO =	go

all: rec

rec:
	${MAKE} -C proto

test: check

check:
	${GO} vet ./...
	${GO} test ./...

.PHONY: all rec test check
