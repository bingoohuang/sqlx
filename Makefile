.PHONY: default test
all: default test

gosec:
	go get github.com/securego/gosec/cmd/gosec
security:
	@gosec ./...
	@echo "[OK] Go security check was completed!"

init:
	export GOPROXY=https://goproxy.cn

default: init
	go fmt ./...&&revive .&&goimports -w .&&golangci-lint run --enable-all&&go install -ldflags="-s -w" ./...

install: init
	go install -ldflags="-s -w" ./...

test: init
	go test ./...
