GO=go

.PHONY: run

all: run

run:
	${GO} build -v -o run ./cmd/run

check: test

test:
	${GO} vet ./...
	${GO} test -v ./...

clean:
	rm -f run

.PHONY: all run check test clean
