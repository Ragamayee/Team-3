package mw

import (
	"time"

	"github.com/sony/gobreaker"
)

func NewBreaker(name string) *gobreaker.CircuitBreaker {
	st := gobreaker.Settings{
		Name:        name,
		MaxRequests: 3, // half-open
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// open circuit if >=5 requests and >50% failures
			fail := counts.TotalFailures
			total := counts.Requests
			return total >= 5 && float64(fail)/float64(total) > 0.5
		},
	}
	return gobreaker.NewCircuitBreaker(st)
}
