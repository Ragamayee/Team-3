package store

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type manifest struct {
	Segments []string `json:"segments"`
}

func loadOrCreateManifest(path string) (*manifest, error) {
	if _, err := os.Stat(path); err == nil {
		b, err := os.ReadFile(path)
		if err != nil { return nil, err }
		var m manifest
		if err := json.Unmarshal(b, &m); err != nil { return nil, err }
		return &m, nil
	}
	m := &manifest{Segments: []string{}}
	return m, m.Save(path)
}

func (m *manifest) Save(path string) error {
	b, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(path, b, 0o644)
}

func (m *manifest) Add(seg string) { m.Segments = append(m.Segments, seg) }

func (m *manifest) nextName() string {
	// find next incrementing number like 000001.sst
	max := 0
	for _, s := range m.Segments {
		n := strings.TrimSuffix(s, ".sst")
		i, _ := strconv.Atoi(n)
		if i > max { max = i }
	}
	return fmt.Sprintf("%06d.sst", max+1)
}
