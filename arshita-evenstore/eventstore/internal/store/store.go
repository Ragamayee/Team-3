package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type Event struct {
	Key   string
	TS    int64
	Value json.RawMessage
}

type Options struct {
	DataDir          string
	MemtableMaxItems int
}

type LSMStore struct {
	opts Options

	mu       sync.RWMutex
	mem      *memtable
	wal      *wal
	manifest *manifest
}

func NewLSMStore(opts Options) (*LSMStore, error) {
	if opts.MemtableMaxItems <= 0 { opts.MemtableMaxItems = 50000 }
	if opts.DataDir == "" { return nil, errors.New("DataDir required") }

	if err := os.MkdirAll(filepath.Join(opts.DataDir, "sst"), 0o755); err != nil {
		return nil, err
	}

	mf, err := loadOrCreateManifest(filepath.Join(opts.DataDir, "manifest.json"))
	if err != nil { return nil, err }

	wal, err := openWAL(filepath.Join(opts.DataDir, "wal.log"))
	if err != nil { return nil, err }

	mem := newMemtable(opts.MemtableMaxItems)

	s := &LSMStore{opts: opts, mem: mem, wal: wal, manifest: mf}

	// recover from WAL into memtable (best-effort)
	if err := wal.Replay(func(e Event) { s.mem.upsert(e) }); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *LSMStore) Put(ctx context.Context, e Event) error {
	if e.Key == "" || e.TS == 0 || len(e.Value) == 0 {
		return errors.New("invalid event")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Append(e); err != nil {
		return err
	}

	s.mem.upsert(e)

	if s.mem.full() {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *LSMStore) Get(ctx context.Context, key string) (Event, bool, error) {
	s.mu.RLock()
	ev, ok := s.mem.get(key)
	s.mu.RUnlock()
	if ok {
		return ev, true, nil
	}

	// search SSTables (newest first)
	s.mu.RLock()
	files := append([]string(nil), s.manifest.Segments...)
	s.mu.RUnlock()

	for i := len(files) - 1; i >= 0; i-- {
		path := filepath.Join(s.opts.DataDir, "sst", files[i])
		ev, ok, err := sstableGet(path, key)
		if err != nil { return Event{}, false, err }
		if ok { return ev, true, nil }
	}
	return Event{}, false, nil
}

func (s *LSMStore) Replay(ctx context.Context, from, to int64) (<-chan Event, error) {
	out := make(chan Event, 128)

	// Simple approach:
	// 1) Collect eligible events from memtable and all sstables
	// 2) Sort by TS
	go func() {
		defer close(out)
		var all []Event

		s.mu.RLock()
		all = append(all, s.mem.rangeByTS(from, to)...)
		files := append([]string(nil), s.manifest.Segments...)
		s.mu.RUnlock()

		for _, f := range files {
			path := filepath.Join(s.opts.DataDir, "sst", f)
			evs, err := sstableRangeTS(path, from, to)
			if err != nil { continue }
			all = append(all, evs...)
		}

		// in-memory sort by TS (stable)
		sortByTS(all)

		for _, e := range all {
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (s *LSMStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem.len() > 0 {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	return s.wal.Close()
}

func (s *LSMStore) flushLocked() error {
	// snapshot memtable
	items := s.mem.snapshotSortedByKey()
	if len(items) == 0 { return nil }

	// new segment filename
	segName := s.manifest.nextName()

	path := filepath.Join(s.opts.DataDir, "sst", segName)
	if err := sstableWrite(path, items); err != nil {
		return err
	}

	// rotate wal (truncate) & clear mem
	if err := s.wal.Rotate(); err != nil { return err }
	s.mem.clear()

	// update manifest
	s.manifest.Add(segName)
	return s.manifest.Save(filepath.Join(s.opts.DataDir, "manifest.json"))
}
