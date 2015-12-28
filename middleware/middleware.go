package middleware

import (
	"net/http"
)

type SharedMiddleware struct {
	Auth func(http.Handler) http.Handler
}
