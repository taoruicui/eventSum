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

CREATE := CREATE ROLE eventsum WITH SUPERUSER CREATEDB CREATEROLE LOGIN ENCRYPTED PASSWORD 'eventsum';

local_db_server_start:
	@echo 'Starting local server...'; pg_ctl -D /usr/local/var/postgres start &> /dev/null; \
	EXIT_CODE=$$?; \
	if [ "$$EXIT_CODE" -eq 1 ]; then \
		echo "Local postgres server is running already...."; \
	else \
		echo "Successfully started local psql server"; \
	fi

local_db_server_stop:
	@echo 'Stopping local server'; pg_ctl -D /usr/local/var/postgres stop &> /dev/null; \
	EXIT_CODE=$$?; \
    if [ "$$EXIT_CODE" -eq 1 ]; then \
        echo "Local postgres server is not running, start it first...."; \
    else \
        echo "Successfully stopped up local psql server"; \
    fi

clean: imports fmt lint

run:
	go run cmd/example.go -c `pwd`/config/config.json