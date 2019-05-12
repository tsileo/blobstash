# microformats

[![GoDoc](https://godoc.org/willnorris.com/go/webmention?status.svg)](https://godoc.org/willnorris.com/go/microformats)
[![Build Status](https://travis-ci.org/willnorris/microformats.svg)](https://travis-ci.org/willnorris/microformats)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)

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

[Parse]: https://godoc.org/willnorris.com/go/microformats#Parse
[ParseNode]: https://godoc.org/willnorris.com/go/microformats#ParseNode
[io.Reader]: https://golang.org/pkg/io/#Reader
[html.Node]: https://godoc.org/golang.org/x/net/html#Node

## License

microformats is released under an [MIT license](LICENSE).  Though portions are
copyright Google, it is not an official Google product.
