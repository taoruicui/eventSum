## Lint and format code
GO_FILES := find . -type f -iname "*.go" | grep -v '^./vendor'
GOIMPORTS := goimports -local=github.com/ContextLogic/eventsum
GETGOLINT := $(shell go get -u golang.org/x/lint/golint 2> /dev/null)
GOLINT := golint
GOFMT := gofmt

imports:
	$(GOIMPORTS) -w $(shell $(GO_FILES))

fmt:
	$(GOFMT) -w -s $(shell $(GO_FILES))

lint:
	${GETGOLINT} ${GOLINT} $(shell $(GO_FILES))

clean: imports fmt lint