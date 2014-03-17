## leakybucket

Leaky bucket implementation in Go with your choice of data storage layer.

## Why

[Leaky buckets](https://en.wikipedia.org/wiki/Leaky_bucket) are useful in a number of settings, especially rate limiting.

## Documentation

[![GoDoc](https://godoc.org/github.com/Clever/leakybucket?status.png)](https://godoc.org/github.com/Clever/leakybucket).

## Tests

leakybucket is built and tested against Go 1.2.
Ensure this is the version of Go you're running with `go version`.
Make sure your GOPATH is set, e.g. `export GOPATH=~/go`.
Clone the repository to a location outside your GOPATH, and symlink it to `$GOPATH/src/github.com/Clever/leakybucket`.
If you have [gvm](https://github.com/moovweb/gvm) installed, you can make this symlink by running the following from the root of where you have cloned the repository: `gvm linkthis github.com/Clever/leakybucket`.

If you have done all of the above, then you should be able to run

```
make
```

If you'd like to see a code coverage report, install the cover tool (`go get code.google.com/p/go.tools/cmd/cover`), make sure `$GOPATH/bin` is in your PATH, and run:

```
COVERAGE=1 make
```

If you'd like to see lint your code, install golint (`go get github.com/golang/lint/golint`) and run:

```
LINT=1 make
```
