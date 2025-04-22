include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS) dynamodb-test
SHELL := /bin/bash
PKG := github.com/Clever/leakybucket
PKGS := $(shell go list ./... | grep -v /dynamodb | grep -v /vendor)
$(eval $(call golang-version-check,1.24))

export REDIS_URL ?= localhost:6379

dynamodb-test:
	./run_dynamodb_test.sh

test: $(PKGS) dynamodb-test
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)


install_deps:
	go mod vendor
