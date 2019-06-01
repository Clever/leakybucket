include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash
PKG := github.com/Clever/leakybucket
PKGS := $(shell go list ./...)
$(eval $(call golang-version-check,1.12))

export REDIS_URL ?= localhost:6379

test: $(PKGS)
$(PKGS): golang-test-all-deps
	go get -d -t $@
	$(call golang-test-all,$@)


install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)