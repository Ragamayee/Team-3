package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"eventstore/internal/store"
)

type ProducerConfig struct {
	BrokersCSV string
	Topic      string
}

type Producer struct {
	w *kafka.Writer
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	brs := splitCSV(cfg.BrokersCSV)
	if len(brs) == 0 { return nil, errors.New("no brokers") }
	w := &kafka.Writer{
		Addr:         kafka.TCP(brs...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 50 * time.Millisecond,
	}
	return &Producer{w: w}, nil
}

func (p *Producer) Publish(ctx context.Context, payload []byte) error {
	return p.w.WriteMessages(ctx, kafka.Message{Value: payload})
}

func (p *Producer) Close() error { return p.w.Close() }

type ConsumerConfig struct {
	BrokersCSV string
	Topic      string
	GroupID    string
}

type Consumer struct {
	r *kafka.Reader
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	brs := splitCSV(cfg.BrokersCSV)
	if len(brs) == 0 { return nil, errors.New("no brokers") }
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brs,
		Topic:     cfg.Topic,
		GroupID:   cfg.GroupID,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})
	return &Consumer{r: r}, nil
}

func (c *Consumer) Consume(handle func(store.Event) error) {
	for {
		m, err := c.r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("kafka read: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var dto struct {
			Key   string          `json:"key"`
			TS    int64           `json:"ts"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(m.Value, &dto); err != nil {
			log.Printf("kafka bad json: %v", err)
			continue
		}
		_ = handle(store.Event{Key: dto.Key, TS: dto.TS, Value: []byte(dto.Value)})
	}
}

func (c *Consumer) Close() error { return c.r.Close() }

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" { out = append(out, p) }
	}
	return out
}
