package store

import "sort"

type memtable struct {
	maxItems int
	data     map[string]Event
}

func newMemtable(max int) *memtable {
	return &memtable{maxItems: max, data: make(map[string]Event, max)}
}

func (m *memtable) upsert(e Event) {
	cur, ok := m.data[e.Key]
	if !ok || e.TS >= cur.TS {
		m.data[e.Key] = e
	}
}

func (m *memtable) get(key string) (Event, bool) {
	e, ok := m.data[key]
	return e, ok
}

func (m *memtable) full() bool { return len(m.data) >= m.maxItems }
func (m *memtable) len() int   { return len(m.data) }

func (m *memtable) clear() { m.data = make(map[string]Event, m.maxItems) }

func (m *memtable) snapshotSortedByKey() []Event {
	out := make([]Event, 0, len(m.data))
	for _, e := range m.data { out = append(out, e) }
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (m *memtable) rangeByTS(from, to int64) []Event {
	out := make([]Event, 0, len(m.data))
	for _, e := range m.data {
		if e.TS >= from && e.TS <= to { out = append(out, e) }
	}
	sortByTS(out)
	return out
}

func sortByTS(v []Event) {
	sort.SliceStable(v, func(i, j int) bool {
		if v[i].TS == v[j].TS { return v[i].Key < v[j].Key }
		return v[i].TS < v[j].TS
	})
}
