package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"eventstore/internal/store"
)

type Publisher func(ctx context.Context, payload []byte) error

type HTTP struct {
	store     *store.LSMStore
	publishFn Publisher // may be nil
	mux       *http.ServeMux
}

func NewHTTP(s *store.LSMStore, p Publisher) *HTTP {
	h := &HTTP{store: s, publishFn: p, mux: http.NewServeMux()}
	h.routes()
	return h
}

func (h *HTTP) routes() {
	h.mux.HandleFunc("POST /events", h.postEvent)
	h.mux.HandleFunc("GET /events/", h.getByKey) // /events/{key}
	h.mux.HandleFunc("GET /events", h.replay)    // /events?from=&to=
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

type eventDTO struct {
	Key   string          `json:"key"`
	TS    int64           `json:"ts"`
	Value json.RawMessage `json:"value"`
}

func (h *HTTP) postEvent(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var in eventDTO
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	if in.Key == "" || in.TS == 0 || len(in.Value) == 0 {
		http.Error(w, "key, ts, value required", 400)
		return
	}

	ev := store.Event{Key: in.Key, TS: in.TS, Value: []byte(in.Value)}
	if err := h.store.Put(r.Context(), ev); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if h.publishFn != nil {
		b, _ := json.Marshal(in)
		if err := h.publishFn(r.Context(), b); err != nil {
			w.Header().Set("X-Publish-Error", err.Error())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	io.WriteString(w, `{"ok":true}`)
}

func (h *HTTP) getByKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/events/")
	if key == "" {
		http.Error(w, "missing key", 400)
		return
	}
	ev, ok, err := h.store.Get(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	out := eventDTO{Key: ev.Key, TS: ev.TS, Value: ev.Value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (h *HTTP) replay(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, to, err := parseRange(q.Get("from"), q.Get("to"))
	if err != nil {
		http.Error(w, "invalid from/to", 400)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	ch, err := h.store.Replay(r.Context(), from, to)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	enc := json.NewEncoder(w)
	for ev := range ch {
		_ = enc.Encode(eventDTO{Key: ev.Key, TS: ev.TS, Value: ev.Value})
	}
}

func parseRange(fs, ts string) (int64, int64, error) {
	if fs == "" || ts == "" {
		return 0, 0, errors.New("missing")
	}
	f, err := strconv.ParseInt(fs, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if t < f {
		return 0, 0, errors.New("to<from")
	}
	return f, t, nil
}

// Helper to get current ms (not strictly used, kept for potential extensions)
func nowMs() int64 { return time.Now().UnixMilli() }
