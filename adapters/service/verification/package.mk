GO ?= go

.PHONY: package-contract interoperability

package-contract:
	test -s README.md
	test -s CHANGELOG.md
	test -s LICENSE
	grep -q '^## API reference$$' docs/README.md
	grep -q '^## FAQ$$' docs/reference.md
	$(GO) doc .
	$(GO) test -run '^Example' .

interoperability:
	$(GO) test -count=1 -race -tags=interoperability ./...
