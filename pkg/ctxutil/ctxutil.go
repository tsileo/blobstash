package ctxutil // import "a4.io/blobstash/pkg/ctxutil"

import (
	"context"

	"a4.io/blobstash/pkg/auth"
)

const (
	StashNameHeader        = "BlobStash-Stash-Name"
	FileTreeHostnameHeader = "BlobStash-FileTree-Hostname"
	NamespaceHeader        = "BlobStash-Namespace"
)

type key int

const (
	stashNamekey = iota
	filetreeHostnameKey
	namespaceKey
	authKey
)

func WithStashName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, stashNamekey, name)
}

func StashName(ctx context.Context) (string, bool) {
	h, ok := ctx.Value(stashNamekey).(string)
	return h, ok
}

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

type actionResource struct {
	action, resource string
}

func WithAuth(ctx context.Context, a *auth.Auth) context.Context {
	return context.WithValue(ctx, authKey, a)
}

func Auth(ctx context.Context) (*auth.Auth, bool) {
	a, ok := ctx.Value(authKey).(*auth.Auth)
	return a, ok
}
