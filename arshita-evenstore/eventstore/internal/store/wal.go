package store

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type wal struct {
	mu  sync.Mutex
	f   *os.File
	wr  *bufio.Writer
	path string
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil { return nil, err }
	return &wal{f: f, wr: bufio.NewWriterSize(f, 1<<20), path: path}, nil
}

func (w *wal) Append(e Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, _ := json.Marshal(e)
	if _, err := w.wr.Write(b); err != nil { return err }
	if err := w.wr.WriteByte('\n'); err != nil { return err }
	return w.wr.Flush()
}

func (w *wal) Replay(emit func(Event)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// open read-only
	f, err := os.Open(w.path)
	if err != nil { return err }
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<25)
	for sc.Scan() {
		var e Event
		if err := json.Unmarshal(sc.Bytes(), &e); err == nil && e.Key != "" {
			emit(e)
		}
	}
	return sc.Err()
}

func (w *wal) Rotate() error {
	// truncate file
	if err := w.wr.Flush(); err != nil { return err }
	if err := w.f.Truncate(0); err != nil { return err }
	_, err := w.f.Seek(0, 0)
	return err
}

func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.wr.Flush()
	return w.f.Close()
}
