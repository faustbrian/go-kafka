GO ?= go

.PHONY: interoperability

interoperability:
	$(GO) test -count=1 -race -tags=interoperability ./...
