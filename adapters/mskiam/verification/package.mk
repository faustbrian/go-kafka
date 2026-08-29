GO ?= go

.PHONY: package-contract conformance interoperability msk-interoperability specification

package-contract:
	test -s README.md
	test -s CHANGELOG.md
	test -s LICENSE
	grep -q '^## API reference$$' docs/reference.md
	grep -q '^## When to use this adapter$$' docs/reference.md
	grep -q '^## FAQ$$' docs/reference.md
	$(GO) doc .
	$(GO) test -run '^Example$$' .
	bash scripts/check-specification.sh

specification:
	bash scripts/check-specification.sh

conformance:
	$(GO) test -count=1 -run '^TestProviderGeneratesOwnedExpiringMSKIAMToken$$' ./...
	$(GO) test -count=1 -tags=msk -run '^(TestMSKCompatibilityConfigRejectsUnboundedInputs|TestMSKControlPlaneValidation)$$' .

interoperability:
	$(GO) test -count=1 -run '^TestProviderGeneratesOwnedExpiringMSKIAMToken$$' ./...
	$(GO) test -count=1 -tags=msk -run '^(TestMSKCompatibilityConfigRejectsUnboundedInputs|TestMSKControlPlaneValidation)$$' .

msk-interoperability:
	GO='$(GO)' ./scripts/check-msk-compatibility.sh
