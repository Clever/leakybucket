SHELL := /bin/bash
PKG = github.com/Clever/leakybucket
SUBPKGS = $(shell ls -d */ | sed -e s:\/$$::)
SUBPKGPATHS = $(addprefix $(PKG)/,$(SUBPKGS))
PKGS = $(PKG) $(SUBPKGPATHS)
.PHONY: test $(PKGS) $(SUBPKGS)

test: $(PKGS)

$(PKGS):
ifeq ($(LINT),1)
	golint $(GOPATH)/src/$@*/**.go
endif
	go get -d -t $@
ifeq ($(COVERAGE),1)
	go test -cover -coverprofile=$(GOPATH)/src/$@/c.out $@ -test.v
	go tool cover -html=$(GOPATH)/src/$@/c.out
else
	go test $@ -test.v
endif

$(SUBPKGS): %: $(addprefix $(PKG)/, %)
