SHELL := /bin/bash
PKG = github.com/Clever/leakybucket
SUBPKGSREL = memory redis
SUBPKGS = $(addprefix $(PKG)/,$(SUBPKGSREL))
PKGS = $(PKG) $(SUBPKGS)
.PHONY: test $(PKGS) $(SUBPKGSREL)

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

$(SUBPKGSREL): %: $(addprefix $(PKG)/, %)
