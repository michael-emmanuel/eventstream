.PHONY: build test race bench vet fmt check cover cluster-up cluster-down cluster-clean docker-up docker-down loadgen fuzz

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -run '^$$' -bench . -benchmem ./internal/record ./internal/log ./internal/routing

fuzz:
	go test -run '^$$' -fuzz FuzzDecode -fuzztime 30s ./internal/record

vet:
	go vet ./...

fmt:
	gofmt -w .

# check is what CI runs.
check:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...
	go test -race -count=1 ./...

cover:
	go test -coverprofile=coverage.out ./internal/... ./pkg/... && go tool cover -func=coverage.out | tail -1

cluster-up:
	scripts/cluster.sh start 3

cluster-down:
	scripts/cluster.sh stop

cluster-clean:
	scripts/cluster.sh clean

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

loadgen: build
	bin/loadgen -topic loadtest -rate 10000 -duration 30s
