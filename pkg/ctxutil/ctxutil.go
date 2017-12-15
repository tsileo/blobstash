package ctxutil // import "a4.io/blobstash/pkg/ctxutil"

import (
	"context"
)

const NamespaceHeader = "BlobStash-Namespace"

const FileTreeHostnameHeader = "BlobStash-FileTree-Hostname"

type key int

const (
	namespaceKey        key = 0
	filetreeHostnameKey key = 1
)

func WithFileTreeHostname(ctx context.Context, hostname string) context.Context {
	return context.WithValue(ctx, filetreeHostnameKey, hostname)
}

func FileTreeHostname(ctx context.Context) (string, bool) {
	h, ok := ctx.Value(filetreeHostnameKey).(string)
	return h, ok
}

func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceKey, namespace)
}

func Namespace(ctx context.Context) (string, bool) {
	namespace, ok := ctx.Value(namespaceKey).(string)
	return namespace, ok
}
