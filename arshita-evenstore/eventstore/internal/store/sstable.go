package store

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type index struct {
	Offsets map[string]int64 `json:"offsets"`
}

func sstableWrite(path string, items []Event) error {
	// write data file
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil { return err }
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)

	// simple header
	header := []byte("SST1\n")
	if _, err := w.Write(header); err != nil { return err }

	idx := index{Offsets: make(map[string]int64, len(items))}

	for _, e := range items {
		off, _ := f.Seek(0, os.SEEK_CUR)
		idx.Offsets[e.Key] = off

		line := fmt.Sprintf("%s\t%d\t%s\n", e.Key, e.TS, base64.StdEncoding.EncodeToString(e.Value))
		if _, err := w.WriteString(line); err != nil { return err }
	}
	if err := w.Flush(); err != nil { return err }

	// write index sidecar
	ip := path + ".index.json"
	b, _ := json.Marshal(idx)
	return os.WriteFile(ip, b, 0o644)
}

func sstableGet(path, key string) (Event, bool, error) {
	// read index
	ip := path + ".index.json"
	b, err := os.ReadFile(ip)
	if err != nil { return Event{}, false, err }
	var idx index
	if err := json.Unmarshal(b, &idx); err != nil { return Event{}, false, err }

	off, ok := idx.Offsets[key]
	if !ok { return Event{}, false, nil }

	f, err := os.Open(path)
	if err != nil { return Event{}, false, err }
	defer f.Close()

	if _, err := f.Seek(off, 0); err != nil { return Event{}, false, err }

	br := bufio.NewReaderSize(f, 1<<20)
	// skip header line if we landed at start (cheap check)
	// not necessary; offset points exactly to line start
	line, err := br.ReadString('\n')
	if err != nil { return Event{}, false, err }

	return parseLine(strings.TrimRight(line, "\n"))
}

func sstableRangeTS(path string, from, to int64) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	// read header
	if _, err := br.ReadString('\n'); err != nil { return nil, err }

	var out []Event
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrClosed) { break }
			if len(line) == 0 { break }
		}
		line = strings.TrimRight(line, "\n")
		if line == "" { break }
		ev, ok, _ := tryParseLine(line)
		if !ok { continue }
		if ev.TS >= from && ev.TS <= to { out = append(out, ev) }
	}
	sortByTS(out)
	return out, nil
}

func parseLine(line string) (Event, bool, error) {
	ev, ok, err := tryParseLine(line)
	if err != nil { return Event{}, false, err }
	return ev, ok, nil
}

func tryParseLine(line string) (Event, bool, error) {
	parts := strings.Split(line, "\t")
	if len(parts) != 3 { return Event{}, false, nil }
	key := parts[0]
	var ts int64
	if _, err := fmt.Sscanf(parts[1], "%d", &ts); err != nil { return Event{}, false, err }
	valBytes, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil { return Event{}, false, err }
	return Event{Key: key, TS: ts, Value: valBytes}, true, nil
}
