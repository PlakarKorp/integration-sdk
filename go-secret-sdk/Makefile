GO = go

all: rot13 secrets

gen: secret.pb.go

secret.pb.go secret_grpc.pb.go: secret.proto
	protoc --proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		./secret.proto

rot13:
	${GO} build -v ./cmd/rot13

secrets:
	${GO} build -v ./cmd/secrets

check: test

test:
	${GO} vet ./...
	${GO} test -v ./...

clean:
	rm -f run

.PHONY: all rot13 secrets check test clean
