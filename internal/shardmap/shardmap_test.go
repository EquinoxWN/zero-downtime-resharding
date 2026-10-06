package shardmap

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func base() Map {
	return Map{
		Shards: map[ShardID]Shard{"a": {ID: "a", DSN: "postgres://a"}, "b": {ID: "b", DSN: "postgres://b"}},
		Tenants: map[int64]Placement{
			1: {Shard: "a"},
			2: {Shard: "a", MovingTo: "b", Phase: Copying},
			3: {Shard: "b"},
		},
	}
}

func TestLookupFindsTheServingShard(t *testing.T) {
	m := base()
	s, p, err := m.Lookup(2)
	if err != nil || s.ID != "a" || p.MovingTo != "b" {
		t.Fatalf("Lookup(2) = %v %v %v: a moving tenant is served by its source until cutover", s, p, err)
	}
	if _, _, err := m.Lookup(9); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("unknown tenant: %v", err)
	}
	if got := m.TenantsOn("a"); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("TenantsOn(a) = %v", got)
	}
}

func TestValidateRejectsInconsistentMaps(t *testing.T) {
	if err := base().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := map[string]func(*Map){
		"unknown shard":         func(m *Map) { m.Tenants[1] = Placement{Shard: "z"} },
		"move to itself":        func(m *Map) { m.Tenants[1] = Placement{Shard: "a", MovingTo: "a", Phase: Copying} },
		"move without a phase":  func(m *Map) { m.Tenants[1] = Placement{Shard: "a", MovingTo: "b"} },
		"phase without a move":  func(m *Map) { m.Tenants[1] = Placement{Shard: "a", Phase: Replicating} },
		"unknown phase":         func(m *Map) { m.Tenants[1] = Placement{Shard: "a", MovingTo: "b", Phase: "flying"} },
		"move to unknown shard": func(m *Map) { m.Tenants[1] = Placement{Shard: "a", MovingTo: "z", Phase: Copying} },
		"zero tenant":           func(m *Map) { m.Tenants[0] = Placement{Shard: "a"} },
		"shard without dsn":     func(m *Map) { m.Shards["c"] = Shard{ID: "c"} },
	}
	for name, breakIt := range bad {
		m := base()
		breakIt(&m)
		if err := m.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Validate = %v", name, err)
		}
	}
}

func TestCloneIsDeep(t *testing.T) {
	m := base()
	c := m.Clone()
	c.Tenants[1] = Placement{Shard: "b"}
	delete(c.Shards, "b")
	if m.Tenants[1].Shard != "a" || len(m.Shards) != 2 {
		t.Fatal("Clone shares maps with the original")
	}
}

func TestUpdateBumpsTheVersionAndRefusesInvalidMaps(t *testing.T) {
	ctx := context.Background()
	s := &MemoryStore{}
	m, err := Update(ctx, s, func(m *Map) error { *m = base(); return nil })
	if err != nil || m.Version != 1 {
		t.Fatalf("first update: %v %v", m.Version, err)
	}
	m, err = Update(ctx, s, func(m *Map) error { m.Tenants[4] = Placement{Shard: "b"}; return nil })
	if err != nil || m.Version != 2 {
		t.Fatalf("second update: %v %v", m.Version, err)
	}
	if _, err := Update(ctx, s, func(m *Map) error { m.Tenants[5] = Placement{Shard: "nope"}; return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid update: %v", err)
	}
	if got, _ := s.Load(ctx); got.Version != 2 || len(got.Tenants) != 4 {
		t.Fatalf("a refused update must not be stored: %+v", got)
	}
	if err := s.CompareAndSwap(ctx, 1, Map{Version: 2}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale CAS: %v", err)
	}
}

func TestConcurrentUpdatesNeverLoseOne(t *testing.T) {
	ctx := context.Background()
	s := &MemoryStore{}
	if _, err := Update(ctx, s, func(m *Map) error { *m = base(); return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Update(ctx, s, func(m *Map) error { m.Tenants[int64(100+i)] = Placement{Shard: "a"}; return nil })
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	ok := 0
	for err := range errs {
		if err == nil {
			ok++
		} else if !errors.Is(err, ErrVersionConflict) {
			t.Fatal(err)
		}
	}
	m, _ := s.Load(ctx)
	if int(m.Version) != 1+ok || len(m.Tenants) != 3+ok {
		t.Fatalf("version %d and %d tenants after %d successful updates", m.Version, len(m.Tenants), ok)
	}
}
