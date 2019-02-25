#!/bin/bash
mkdir releases >/dev/null 2>&1
echo '
  darwin   amd64    OS X
  freebsd  amd64    FreeBSD 64-bit
  linux    amd64    Linux 64-bit
  linux    arm64    Linux ARMv8
  linux    mipsle   Linux MIPS-LE
' | {
  while read os arch label; do
    [ -n "$os" ] || continue

    exename="blobstash-${os}-${arch}"
    [ "$os" != "windows" ] || exename="${exename}.exe"
    echo "Building $exename"
    env GOOS=$os GOARCH=$arch go build -mod=vendor -o "releases/$exename" || {
    echo "FAILED FOR $os $arch" >&2
    continue
    }
    echo "Done"
done
}
