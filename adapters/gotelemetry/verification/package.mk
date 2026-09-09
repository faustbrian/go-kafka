GO ?= go

.PHONY: package-contract

package-contract:
	bash scripts/check-docs.sh
