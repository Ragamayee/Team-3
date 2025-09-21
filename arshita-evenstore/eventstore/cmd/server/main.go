package main

import (
	"context"
	"eventstore/internal/api"
	"eventstore/internal/kafka"
	"eventstore/internal/mw"
	"eventstore/internal/store"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	// Config via env (with sane defaults)
	addr := env("HTTP_ADDR", ":8080")
	dataDir := env("DATA_DIR", "./data")
	memLimit := envInt("MEMTABLE_MAX_ITEMS", 50000)
	kafkaBrokers := env("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := env("KAFKA_TOPIC", "events")
	consumeTopic := env("KAFKA_CONSUME_TOPIC", "events-in") // separate input topic
	kafkaEnabled := env("KAFKA_ENABLED", "true") == "true"

	// Ensure data dir
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}

	// Create store (LSM-ish)
	lsm, err := store.NewLSMStore(store.Options{
		DataDir:          dataDir,
		MemtableMaxItems: memLimit,
	})
	if err != nil {
		log.Fatalf("store init: %v", err)
	}
	defer lsm.Close()

	// Kafka (optional but enabled by default)
	var kp *kafka.Producer
	var kc *kafka.Consumer
	if kafkaEnabled {
		kp, err = kafka.NewProducer(kafka.ProducerConfig{
			BrokersCSV: kafkaBrokers,
			Topic:      kafkaTopic,
		})
		if err != nil {
			log.Printf("kafka producer init failed (will continue without publish): %v", err)
		}

		// Circuit breaker wraps producer Publish()
		cb := mw.NewBreaker("kafka-producer")
		publish := func(ctx context.Context, b []byte) error {
			if kp == nil {
				return nil
			}
			_, err := cb.Execute(func() (any, error) {
				return nil, kp.Publish(ctx, b)
			})
			return err
		}

		// Attach HTTP with publish hook
		handler := api.NewHTTP(lsm, publish)

		// Rate limiter (100 rps, burst 200)
		rl := mw.NewRateLimiter(100, 200)
		mux := rl.Wrap(handler)

		srv := &http.Server{Addr: addr, Handler: mux}
		go func() {
			log.Printf("HTTP listening on %s", addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("http: %v", err)
			}
		}()

		// Kafka consumer to ingest external events
		kc, err = kafka.NewConsumer(kafka.ConsumerConfig{
			BrokersCSV: kafkaBrokers,
			Topic:      consumeTopic,
			GroupID:    "eventstore-consumers",
		})
		if err != nil {
			log.Printf("kafka consumer init failed (continuing): %v", err)
		} else {
			go func() {
				log.Printf("Kafka consumer started: topic=%s", consumeTopic)
				kc.Consume(func(e store.Event) error {
					// idempotent: Put is upsert by (key, ts)
					return lsm.Put(context.Background(), e)
				})
			}()
		}

		// graceful shutdown
		waitForShutdown(srv, kp, kc, lsm)
		return
	}

	// If Kafka disabled, just run HTTP with rate limiter and no publish hook.
	handler := api.NewHTTP(lsm, nil)
	rl := mw.NewRateLimiter(100, 200)
	mux := rl.Wrap(handler)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("HTTP listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()
	waitForShutdown(srv, nil, nil, lsm)
}

func waitForShutdown(srv *http.Server, kp *kafka.Producer, kc *kafka.Consumer, lsm *store.LSMStore) {
	// OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if kc != nil {
		_ = kc.Close()
	}
	if kp != nil {
		_ = kp.Close()
	}

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown http: %v", err)
	}
	_ = lsm.Close()
	log.Println("bye")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		_, err := fmtSscanf(v, "%d", &n)
		if err == nil {
			return n
		}
	}
	return def
}

// tiny wrapper avoiding importing fmt into main for just Sscanf
func fmtSscanf(s, format string, a ...any) (int, error) {
	return fmt_sscanf(s, format, a...)
}

// --- minimal sscanf helper ---

func fmt_sscanf(s, format string, a ...any) (int, error) {
	return fmt.Sscanf(s, format, a...)
}
