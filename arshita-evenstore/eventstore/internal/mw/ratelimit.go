package mw

import (
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	lim *rate.Limiter
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{lim: rate.NewLimiter(rate.Limit(rps), burst)}
}

func (rl *RateLimiter) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.lim.Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// If you want a blocking variant (not used here)
func (rl *RateLimiter) Wait(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := rl.lim.Wait(r.Context()); err != nil {
			http.Error(w, "rate limit", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func Sleep(d time.Duration) { time.Sleep(d) } // tiny helper if you need backoff later
