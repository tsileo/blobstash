 #!/bin/bash
mkdir releases >/dev/null 2>&1
echo '
  darwin   amd64    OS X
  freebsd  386      FreeBSD 32-bit
  freebsd  amd64    FreeBSD 64-bit
  linux    386      Linux 32-bit
  linux    amd64    Linux 64-bit
' | {
  while read os arch label; do
    [ -n "$os" ] || continue

    exename="blobstash-${os}-${arch}"
    [ "$os" != "windows" ] || exename="${exename}.exe"
    echo "Building $exename"
    env GOOS=$os GOARCH=$arch go build -o "releases/$exename" || {
    echo "FAILED FOR $os $arch" >&2
    continue
    }
    echo "Done"
done
}
