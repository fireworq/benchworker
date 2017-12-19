BIN=benchworker
SHELL=/bin/bash -O globstar
BUILD_OUTPUT=.
GO=go
BUILD=$$(git describe --always)

build: deps
	${GO} build -ldflags "-X main.Build=$(BUILD)" -o ${BUILD_OUTPUT}/$(BIN) .

deps:
	glide install

clean:
	rm -f $(BIN)
	${GO} clean

.PHONY: build deps clean
