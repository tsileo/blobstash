# microformats

[![GoDoc](https://img.shields.io/static/v1?label=godoc&message=reference&color=blue)](https://pkg.go.dev/willnorris.com/go/microformats)
[![Test Status](https://github.com/willnorris/microformats/workflows/tests/badge.svg)](https://github.com/willnorris/microformats/actions?query=workflow%3Atests)
[![Test Coverage](https://codecov.io/gh/willnorris/microformats/branch/master/graph/badge.svg)](https://codecov.io/gh/willnorris/microformats)

microformats is a go library and tool for parsing [microformats][], supporting
both classic v1 and [v2 syntax][].  It is based on Andy Leap's [original
library][andyleap/microformats].

[microformats]: https://microformats.io/
[v2 syntax]: http://microformats.org/wiki/microformats-2
[andyleap/microformats]: https://github.com/andyleap/microformats

## Usage

Import the package:

``` go
import "willnorris.com/go/microformats"
```

Fetch the HTML contents of a page, and call [Parse][] or [ParseNode][],
depending on what input you have (an [io.Reader][] or an [html.Node][]). See an
example of each in [cmd/gomf/main.go](cmd/gomf/main.go).

[Parse]: https://pkg.go.dev/willnorris.com/go/microformats#Parse
[ParseNode]: https://pkg.go.dev/willnorris.com/go/microformats#ParseNode
[io.Reader]: https://golang.org/pkg/io/#Reader
[html.Node]: https://pkg.go.dev/golang.org/x/net/html#Node

## License

microformats is released under an [MIT license](LICENSE).  Though portions are
copyright Google, it is not an official Google product.
