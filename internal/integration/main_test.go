// Package integration runs the router and the move tool against three real PostgreSQL servers:
// a control database holding the shard map, and two shards.
package integration

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/localpg"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/move"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/schema"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
)

var env struct {
	cluster *localpg.Cluster
	store   shardmap.PGStore
	pools   map[shardmap.ShardID]*pgxpool.Pool
	mu      sync.Mutex
}

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("integration tests need PostgreSQL servers; skipped with -short")
		os.Exit(0)
	}
	c, err := localpg.Start("control", "shard-a", "shard-b")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start PostgreSQL:", err)
		os.Exit(1)
	}
	env.cluster = c
	ctx := context.Background()
	env.pools = map[shardmap.ShardID]*pgxpool.Pool{}
	control, err := pgxpool.New(ctx, c.Servers[0].DSN)
	must(err)
	env.store = shardmap.PGStore{Pool: control}
	must(env.store.Init(ctx))
	_, err = shardmap.Update(ctx, env.store, func(m *shardmap.Map) error {
		for _, s := range c.Servers[1:] {
			id := shardmap.ShardID(s.Name)
			m.Shards[id] = shardmap.Shard{ID: id, DSN: s.DSN}
		}
		return nil
	})
	must(err)
	for _, s := range c.Servers[1:] {
		p, err := pool(ctx, shardmap.Shard{ID: shardmap.ShardID(s.Name), DSN: s.DSN})
		must(err)
		must(schema.Apply(ctx, p))
	}
	code := m.Run()
	for _, p := range env.pools {
		p.Close()
	}
	control.Close()
	c.Stop()
	os.Exit(code)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if env.cluster != nil {
			fmt.Fprintln(os.Stderr, env.cluster.Logs())
			env.cluster.Stop()
		}
		os.Exit(1)
	}
}

// pool returns one shared pool per shard.
func pool(ctx context.Context, s shardmap.Shard) (*pgxpool.Pool, error) {
	env.mu.Lock()
	defer env.mu.Unlock()
	if p, ok := env.pools[s.ID]; ok {
		return p, nil
	}
	p, err := pgxpool.New(ctx, s.DSN)
	if err != nil {
		return nil, err
	}
	env.pools[s.ID] = p
	return p, nil
}

func shard(t *testing.T, id shardmap.ShardID) *pgxpool.Pool {
	t.Helper()
	m, err := env.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := pool(context.Background(), m.Shards[id])
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mover() *move.Mover { return &move.Mover{Store: env.store, Pool: pool} }

// place puts tenants on a shard in the map.
func place(t *testing.T, on shardmap.ShardID, tenants ...int64) {
	t.Helper()
	_, err := shardmap.Update(context.Background(), env.store, func(m *shardmap.Map) error {
		for _, tn := range tenants {
			m.Tenants[tn] = shardmap.Placement{Shard: on}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// seed writes accounts and orders for a tenant directly on a shard.
func seed(t *testing.T, p *pgxpool.Pool, tenant int64, accounts, orders int) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO accounts (tenant_id, id, email)
		SELECT $1::bigint, g, 'user' || g || '@tenant' || $1::bigint || '.example' FROM generate_series(1, $2::int) g`, tenant, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO orders (tenant_id, id, account_id, total, status)
		SELECT $1::bigint, g, 1 + g % $2::int, (g % 997) * 1.25, 'open' FROM generate_series(1, $3::int) g`, tenant, accounts, orders); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, p *pgxpool.Pool, table string, tenant int64) int64 {
	t.Helper()
	var n int64
	if err := p.QueryRow(context.Background(), "SELECT count(*) FROM "+table+" WHERE tenant_id = $1", tenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
