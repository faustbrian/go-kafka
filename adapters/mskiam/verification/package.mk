GO ?= go

.PHONY: package-contract conformance interoperability msk-interoperability specification

package-contract:
	./scripts/check-docs.sh
	./scripts/check-specification.sh

specification:
	./scripts/check-specification.sh

conformance:
	$(GO) test -count=1 -run '^TestProviderGeneratesOwnedExpiringMSKIAMToken$$' ./...
	$(GO) test -count=1 -tags=msk -run '^(TestMSKCompatibilityConfigRejectsUnboundedInputs|TestMSKControlPlaneValidation)$$' .

interoperability:
	$(GO) test -count=1 -run '^TestProviderGeneratesOwnedExpiringMSKIAMToken$$' ./...
	$(GO) test -count=1 -tags=msk -run '^(TestMSKCompatibilityConfigRejectsUnboundedInputs|TestMSKControlPlaneValidation)$$' .

msk-interoperability:
	GO='$(GO)' ./scripts/check-msk-compatibility.sh
