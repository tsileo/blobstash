package ctxutil // import "a4.io/blobstash/pkg/ctxutil"

import (
	"context"
)

const NamespaceHeader = "BlobStash-Namespace"

type key int

const (
	namespaceKey key = 0
)

func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceKey, namespace)
}

func Namespace(ctx context.Context) (string, bool) {
	namespace, ok := ctx.Value(namespaceKey).(string)
	return namespace, ok
}
