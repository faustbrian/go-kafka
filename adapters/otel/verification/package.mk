GO ?= go

.PHONY: package-contract conformance interoperability

package-contract:
	bash scripts/check-docs.sh

conformance:
	$(GO) test -tags=interoperability -run '^TestTraceContextPropagationAcrossApacheKafka$$' -count=1 -timeout=5m .

interoperability:
	$(GO) test -tags=interoperability -count=1 -timeout=20m ./...
