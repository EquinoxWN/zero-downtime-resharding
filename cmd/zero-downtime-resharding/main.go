// Command zero-downtime-resharding starts three local PostgreSQL servers (a control database
// and two shards), loads six tenants, and moves one of them to the other shard while four
// workers keep writing to it through the router.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/localpg"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/move"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/router"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/schema"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/verify"
)

func main() {
	orders := flag.Int("orders", 200000, "orders of the tenant that moves")
	flag.Parse()
	if err := run(context.Background(), *orders); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, orders int) error {
	start := time.Now()
	c, err := localpg.Start("control", "shard-a", "shard-b")
	if err != nil {
		return err
	}
	defer c.Stop()
	fmt.Printf("Started 3 PostgreSQL 17 servers on localhost (control, shard-a, shard-b) in %.1f s\n", time.Since(start).Seconds())

	pools := map[shardmap.ShardID]*pgxpool.Pool{}
	var mu sync.Mutex
	pool := func(ctx context.Context, s shardmap.Shard) (*pgxpool.Pool, error) {
		mu.Lock()
		defer mu.Unlock()
		if p, ok := pools[s.ID]; ok {
			return p, nil
		}
		p, err := pgxpool.New(ctx, s.DSN)
		if err == nil {
			pools[s.ID] = p
		}
		return p, err
	}
	defer func() {
		for _, p := range pools {
			p.Close()
		}
	}()
	control, err := pgxpool.New(ctx, c.Servers[0].DSN)
	if err != nil {
		return err
	}
	defer control.Close()
	store := shardmap.PGStore{Pool: control}
	if err := store.Init(ctx); err != nil {
		return err
	}
	sizes := map[int64]int{1: 20000, 2: 20000, 3: orders, 4: 20000, 5: 20000, 6: 20000}
	m, err := shardmap.Update(ctx, store, func(m *shardmap.Map) error {
		for _, s := range c.Servers[1:] {
			id := shardmap.ShardID(s.Name)
			m.Shards[id] = shardmap.Shard{ID: id, DSN: s.DSN}
		}
		for t := range sizes {
			on := shardmap.ShardID("shard-a")
			if t > 4 {
				on = "shard-b"
			}
			m.Tenants[t] = shardmap.Placement{Shard: on}
		}
		return nil
	})
	if err != nil {
		return err
	}
	a, err := pool(ctx, m.Shards["shard-a"])
	if err != nil {
		return err
	}
	b, err := pool(ctx, m.Shards["shard-b"])
	if err != nil {
		return err
	}
	for _, p := range []*pgxpool.Pool{a, b} {
		if err := schema.Apply(ctx, p); err != nil {
			return err
		}
	}
	tenants := make([]int64, 0, len(sizes))
	for t := range sizes {
		tenants = append(tenants, t)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i] < tenants[j] })
	for _, t := range tenants {
		p := a
		if m.Tenants[t].Shard == "shard-b" {
			p = b
		}
		if _, err := p.Exec(ctx, `INSERT INTO accounts (tenant_id, id, email) SELECT $1::bigint, g, 'user' || g || '@t' || $1::bigint || '.example' FROM generate_series(1, 500) g`, t); err != nil {
			return err
		}
		if _, err := p.Exec(ctx, `INSERT INTO orders (tenant_id, id, account_id, total, status) SELECT $1::bigint, g, 1 + g % 500, (g % 997) * 1.25, 'open' FROM generate_series(1, $2::int) g`, t, sizes[t]); err != nil {
			return err
		}
	}
	fmt.Printf("Shard map v%d: tenants 1-4 on shard-a, 5-6 on shard-b; tenant 3 has %d orders, the others 20,000\n\n", m.Version, orders)

	r, err := router.New(ctx, store, time.Second)
	if err != nil {
		return err
	}
	defer r.Close()
	var writes, failures atomic.Int64
	var latMu sync.Mutex
	var latencies []time.Duration
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			next := int64(10_000_000 * (w + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				err := r.Do(ctx, 3, func(ctx context.Context, tx pgx.Tx) error {
					next++
					if _, err := tx.Exec(ctx, `INSERT INTO orders (tenant_id, id, account_id, total, status) VALUES (3, $1, 1, 12.5, 'live')`, next); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, `UPDATE orders SET total = total + 1, status = 'paid' WHERE tenant_id = 3 AND id = $1`, 1+rng.Intn(orders))
					return err
				})
				if err != nil {
					failures.Add(1)
					continue
				}
				writes.Add(1)
				latMu.Lock()
				latencies = append(latencies, time.Since(t0))
				latMu.Unlock()
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	mv := &move.Mover{Store: store, Pool: pool}
	fmt.Println("Moving tenant 3 from shard-a to shard-b while 4 workers write to it through the router:")
	t0 := time.Now()
	if err := mv.Start(ctx, 3, "shard-b"); err != nil {
		return err
	}
	fmt.Printf("  publication (tenant 3 rows only) and subscription created in %d ms\n", time.Since(t0).Milliseconds())
	copied, err := mv.WaitCopied(ctx, 3, 5*time.Minute)
	if err != nil {
		return err
	}
	var copiedRows int64
	_ = b.QueryRow(ctx, "SELECT count(*) FROM orders WHERE tenant_id = 3").Scan(&copiedRows)
	fmt.Printf("  snapshot copy finished in %.2f s (%d orders on shard-b so far); streaming took over\n", copied.Seconds(), copiedRows)
	fmt.Printf("\n  %-8s %-14s %-18s\n", "time", "lag (bytes)", "writes so far")
	for i := range 6 {
		time.Sleep(500 * time.Millisecond)
		lag, err := mv.Lag(ctx, 3)
		if err != nil {
			return err
		}
		fmt.Printf("  +%.1f s   %-14d %-18d\n", float64(i+1)*0.5, lag, writes.Load())
	}
	close(stop)
	wg.Wait()
	lag, err := mv.WaitCaughtUp(ctx, 3, 0, time.Minute)
	if err != nil {
		return err
	}
	fmt.Printf("\n  writers stopped: %d transactions, %d failed; p99 latency %s; lag now %d bytes\n",
		writes.Load(), failures.Load(), p99(latencies), lag)

	report, err := verify.Tenant(ctx, a, b, 3)
	if err != nil {
		return err
	}
	fmt.Printf("\nTenant 3 on both shards (count and md5 of all rows in key order):\n")
	fmt.Printf("  %-9s %-10s %-10s %-34s %s\n", "table", "shard-a", "shard-b", "checksum", "equal")
	equal := true
	for _, rep := range report {
		fmt.Printf("  %-9s %-10d %-10d %-34s %v\n", rep.Name, rep.CountA, rep.CountB, rep.SumA, rep.Equal())
		equal = equal && rep.Equal()
	}
	var others int64
	_ = b.QueryRow(ctx, "SELECT count(*) FROM orders WHERE tenant_id IN (1, 2, 4)").Scan(&others)
	m, _ = store.Load(ctx)
	p := m.Tenants[3]
	served, _, _ := r.Shard(ctx, 3)
	fmt.Printf("\nRows of tenants 1, 2 and 4 copied to shard-b: %d\n", others)
	fmt.Printf("Shard map v%d: tenant 3 on %s, moving to %s, phase %q; the router still sends it to %s.\n", m.Version, p.Shard, p.MovingTo, p.Phase, served.ID)
	fmt.Println("Next (M2): fence tenant 3's writes for a moment, drain the last changes and flip the map.")
	if err := mv.Abort(ctx, 3); err != nil {
		return err
	}
	if !equal || failures.Load() != 0 || others != 0 {
		return fmt.Errorf("the move was not clean")
	}
	return nil
}

func p99(d []time.Duration) string {
	if len(d) == 0 {
		return "n/a"
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)*99/100].Round(100 * time.Microsecond).String()
}
