GO ?= go

.PHONY: package-contract conformance interoperability

package-contract:
	./scripts/check-docs.sh

conformance:
	$(GO) test -tags=interoperability -run '^(TestPublicConformance|TestAuthenticationProviderFuncConformance|TestObserverPolicyConformance)$$' -count=1 -timeout=5m ./...

interoperability:
	$(GO) test -tags=interoperability -count=1 -timeout=20m ./...
