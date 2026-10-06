package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
)

func setup(t *testing.T) (*shardmap.MemoryStore, *Router) {
	t.Helper()
	ctx := context.Background()
	s := &shardmap.MemoryStore{}
	_, err := shardmap.Update(ctx, s, func(m *shardmap.Map) error {
		m.Shards["a"] = shardmap.Shard{ID: "a", DSN: "postgres://a"}
		m.Shards["b"] = shardmap.Shard{ID: "b", DSN: "postgres://b"}
		m.Tenants[1] = shardmap.Placement{Shard: "a"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(ctx, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return s, r
}

func TestTenantsAreRoutedByTheMap(t *testing.T) {
	_, r := setup(t)
	s, v, err := r.Shard(context.Background(), 1)
	if err != nil || s.ID != "a" || v != 1 {
		t.Fatalf("Shard(1) = %v %d %v", s.ID, v, err)
	}
	for _, bad := range []int64{0, -5} {
		if _, _, err := r.Shard(context.Background(), bad); !errors.Is(err, ErrNoTenant) {
			t.Fatalf("tenant %d: %v", bad, err)
		}
	}
}

func TestANewTenantIsFoundWithoutWaitingForTheCacheToExpire(t *testing.T) {
	store, r := setup(t)
	if _, err := shardmap.Update(context.Background(), store, func(m *shardmap.Map) error {
		m.Tenants[2] = shardmap.Placement{Shard: "b"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s, v, err := r.Shard(context.Background(), 2)
	if err != nil || s.ID != "b" || v != 2 {
		t.Fatalf("Shard(2) = %v %d %v", s.ID, v, err)
	}
	if _, _, err := r.Shard(context.Background(), 3); !errors.Is(err, shardmap.ErrUnknownTenant) {
		t.Fatalf("missing tenant: %v", err)
	}
}

func TestAnExpiredCacheIsReread(t *testing.T) {
	store, r := setup(t)
	r.maxAge = 0
	if _, err := shardmap.Update(context.Background(), store, func(m *shardmap.Map) error {
		m.Tenants[1] = shardmap.Placement{Shard: "b"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := r.Shard(context.Background(), 1); s.ID != "b" {
		t.Fatalf("tenant 1 still routed to %s after the map changed", s.ID)
	}
	if r.Version() != 2 {
		t.Fatalf("Version = %d", r.Version())
	}
}
