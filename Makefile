SHELL := /usr/bin/env bash

.PHONY: check ci cohesion config inventory repository-check workflows

config:
	golib config validate

inventory:
	golib inventory

repository-check:
	golib repository check

workflows:
	golib workflows check

check:
	golib check --all

cohesion:
	golib cohesion check

ci: config inventory cohesion repository-check workflows check
