package session // import "a4.io/blobstash/pkg/session"

import (
	"a4.io/blobstash/pkg/config"
	"github.com/gorilla/sessions"
)

type Session struct {
	sess *sessions.CookieStore
}

func (s *Session) Session() *sessions.CookieStore {
	return s.sess
}

func New(conf *config.Config) *Session {
	return &Session{
		sess: sessions.NewCookieStore([]byte(conf.SecretKey)),
	}
}
