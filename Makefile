SHELL := /usr/bin/env bash

.PHONY: check ci cohesion config inventory repository-check specification-check workflows

config:
	golib config validate

inventory:
	golib inventory

repository-check:
	golib repository check

specification-check:
	golib specification check --online

workflows:
	golib workflows check

check:
	golib check --all

cohesion:
	golib cohesion check

ci: config inventory cohesion repository-check specification-check workflows check
