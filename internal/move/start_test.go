package move

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
)

func memoryMap(t *testing.T) *shardmap.MemoryStore {
	t.Helper()
	s := &shardmap.MemoryStore{}
	if _, err := shardmap.Update(context.Background(), s, func(m *shardmap.Map) error {
		m.Shards["a"] = shardmap.Shard{ID: "a", DSN: "postgres://a"}
		m.Shards["b"] = shardmap.Shard{ID: "b", DSN: "postgres://b"}
		m.Tenants[7] = shardmap.Placement{Shard: "a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAStartThatCannotReachTheShardsLeavesTheTenantStableAndCanBeRetried(t *testing.T) {
	ctx := context.Background()
	store := memoryMap(t)
	down := errors.New("shard unreachable")
	mv := &Mover{Store: store, Pool: func(context.Context, shardmap.Shard) (*pgxpool.Pool, error) { return nil, down }}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := mv.Start(ctx, 7, "b"); !errors.Is(err, down) {
			t.Fatalf("attempt %d: %v, want the pool error", attempt, err)
		}
		m, err := store.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if p := m.Tenants[7]; p != (shardmap.Placement{Shard: "a"}) {
			t.Fatalf("attempt %d left tenant 7 as %+v, want stable on a", attempt, p)
		}
	}
}

func TestMovesThatMakeNoSenseAreRefusedWithoutTouchingAShard(t *testing.T) {
	ctx := context.Background()
	store := memoryMap(t)
	used := false
	mv := &Mover{Store: store, Pool: func(context.Context, shardmap.Shard) (*pgxpool.Pool, error) {
		used = true
		return nil, errors.New("unused")
	}}
	if err := mv.Start(ctx, 7, "a"); !errors.Is(err, ErrSameShard) {
		t.Fatalf("move to its own shard: %v", err)
	}
	if err := mv.Start(ctx, 99, "b"); !errors.Is(err, shardmap.ErrUnknownTenant) {
		t.Fatalf("unknown tenant: %v", err)
	}
	if err := mv.Start(ctx, 7, "zzz"); !errors.Is(err, shardmap.ErrInvalid) {
		t.Fatalf("unknown target shard: %v", err)
	}
	if _, err := shardmap.Update(ctx, store, func(m *shardmap.Map) error {
		m.Tenants[8] = shardmap.Placement{Shard: "a", MovingTo: "b", Phase: shardmap.Copying}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := mv.Start(ctx, 8, "b"); !errors.Is(err, ErrAlreadyMoving) {
		t.Fatalf("a second move of a moving tenant: %v", err)
	}
	if err := mv.Abort(ctx, 7); !errors.Is(err, ErrNotMoving) {
		t.Fatalf("abort of a stable tenant: %v", err)
	}
	if used {
		t.Fatal("a refused move must not connect to any shard")
	}
	if m, _ := store.Load(ctx); m.Version != 2 || m.Tenants[8].Phase != shardmap.Copying {
		t.Fatalf("refused moves changed the map to version %d", m.Version)
	}
}
