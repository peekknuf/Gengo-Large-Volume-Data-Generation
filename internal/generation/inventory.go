package generation

import (
	"fmt"
	"path/filepath"
	"sync"
)

// Inventory records rows accepted by successfully closed writers. Each run owns
// its own inventory, including when callers reuse a Config across runs.
type Inventory struct {
	mu    sync.Mutex
	files map[string]FileRows
}

type FileRows struct{ Rows, FirstKey, LastKey int64 }

func NewInventory() *Inventory { return &Inventory{files: map[string]FileRows{}} }

func (c Config) RecordFile(name string, rows int, first, last int64) error {
	if c.Output == nil {
		return nil
	}
	name = filepath.Base(name)
	c.Output.mu.Lock()
	defer c.Output.mu.Unlock()
	if _, exists := c.Output.files[name]; exists {
		return fmt.Errorf("duplicate output file %s", name)
	}
	c.Output.files[name] = FileRows{Rows: int64(rows), FirstKey: first, LastKey: last}
	return nil
}

func (i *Inventory) File(name string) (FileRows, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	rows, ok := i.files[name]
	return rows, ok
}
