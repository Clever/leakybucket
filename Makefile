include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS) dynamodb-test
SHELL := /bin/bash
PKG := github.com/Clever/leakybucket
PKGS := $(shell go list ./... | grep -v /dynamodb)
$(eval $(call golang-version-check,1.13))

export REDIS_URL ?= localhost:6379

dynamodb-test:
	./run_dynamodb_test.sh

test: $(PKGS) dynamodb-test
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)


install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)
