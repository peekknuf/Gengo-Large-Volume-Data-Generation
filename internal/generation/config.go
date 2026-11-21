package generation

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	legacyrand "math/rand"
	randv2 "math/rand/v2"
	"runtime"
	"time"

	gofakeit "github.com/brianvoe/gofakeit/v6"
	"golang.org/x/sync/errgroup"
)

const AlgorithmVersion = "gengo-v1"
const DefaultRowsPerFile = 100000

// Config is immutable during a run. Streams belong to tables or logical ranges,
// so execution order and worker count do not affect the generated values.
type Config struct {
	Output      *Inventory `json:"-"`
	Seed        uint64     `json:"seed"`
	AsOf        time.Time  `json:"as_of"`
	RowsPerFile int        `json:"rows_per_file"`
	Workers     int        `json:"workers"`
}

func Resolve(seed *uint64, date string, rowsPerFile, workers int) (Config, error) {
	c := Config{RowsPerFile: rowsPerFile, Workers: workers}
	if seed == nil {
		var entropy [8]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return c, err
		}
		c.Seed = binary.LittleEndian.Uint64(entropy[:])
	} else {
		c.Seed = *seed
	}
	if date == "" {
		date = time.Now().UTC().Format(time.DateOnly)
	}
	var err error
	c.AsOf, err = time.Parse(time.DateOnly, date)
	if err != nil {
		return c, fmt.Errorf("as-of must use YYYY-MM-DD: %w", err)
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.RowsPerFile < 1 {
		return fmt.Errorf("rows-per-file must be at least 1")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if c.AsOf.IsZero() || c.AsOf.Year() < 1941 || c.AsOf.Year() > 9999 {
		return fmt.Errorf("as-of must be a date between 1941-01-01 and 9999-12-31")
	}
	if !c.AsOf.Equal(c.AsOf.UTC().Truncate(24 * time.Hour)) {
		return fmt.Errorf("as-of must be midnight UTC")
	}
	return nil
}

// Options preserves existing internal callers that do not supply configuration.
func Options(options []Config) Config {
	if len(options) > 0 {
		return options[0]
	}
	c, err := Resolve(nil, "", DefaultRowsPerFile, runtime.GOMAXPROCS(0))
	if err != nil {
		panic(err)
	}
	return c
}

// StreamSeed pins FNV-1a of the table name, followed by SplitMix64 mixing.
func (c Config) StreamSeed(table string, index uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(table))
	x := c.Seed ^ h.Sum64() ^ (index * 0x9e3779b97f4a7c15)
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (c Config) Rand(table string, index uint64) *legacyrand.Rand {
	return legacyrand.New(legacyrand.NewSource(int64(c.StreamSeed(table, index))))
}

func (c Config) RandV2(table string, index uint64) *randv2.Rand {
	return randv2.New(randv2.NewPCG(c.StreamSeed(table, index), c.StreamSeed(table, index+1)))
}

func (c Config) Faker(table string) *gofakeit.Faker {
	// gofakeit treats zero as an unseeded request.
	seed := int64(c.StreamSeed(table, 0))
	if seed == 0 {
		seed = 1
	}
	return gofakeit.New(seed)
}

type Partition struct{ Index, Start, Count int }

func Partitions(ctx context.Context, total int, c Config, write func(context.Context, Partition) error) error {
	parent := ctx
	group, ctx := errgroup.WithContext(ctx)
	if total == 0 {
		return parent.Err()
	}
	group.SetLimit(min(c.Workers, 1+(total-1)/c.RowsPerFile))
	for start, index := 0, 0; start < total; index++ {
		if err := ctx.Err(); err != nil {
			break
		}
		p := Partition{Index: index, Start: start, Count: min(c.RowsPerFile, total-start)}
		group.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return write(ctx, p)
		})
		start += p.Count
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return parent.Err()
}
