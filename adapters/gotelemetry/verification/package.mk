GO ?= go

.PHONY: package-contract conformance interoperability specification

package-contract:
	./scripts/check-docs.sh
	./scripts/check-specification.sh

specification:
	./scripts/check-specification.sh

conformance:
	$(GO) test -tags=interoperability -run '^TestTraceContextPropagationAcrossApacheKafka$$' -count=1 -timeout=5m .

interoperability:
	$(GO) test -tags=interoperability -count=1 -timeout=20m ./...
