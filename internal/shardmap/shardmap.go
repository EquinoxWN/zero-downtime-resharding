// Package shardmap is the versioned map from tenant to shard that every router reads. Every
// change produces a new version and is written with compare-and-swap, so two operators can never
// both move the same tenant, and a router can tell that its copy is stale.
package shardmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
)

// ShardID names a shard, for example "shard-a".
type ShardID string

// Shard is one PostgreSQL database holding some tenants.
type Shard struct {
	ID  ShardID `json:"id"`
	DSN string  `json:"dsn"` // connection string; never printed (it holds a password)
}

// Phase is where a tenant is in a move.
type Phase string

// Move phases. M1 implements copying and replicating; fencing and the map flip are M2.
const (
	Stable      Phase = ""
	Copying     Phase = "copying"     // snapshot copy of the tenant's rows to the target is running
	Replicating Phase = "replicating" // the copy is done; logical replication streams new changes
)

// Placement says where a tenant lives and whether it is moving.
type Placement struct {
	Shard    ShardID `json:"shard"`
	MovingTo ShardID `json:"moving_to,omitempty"`
	Phase    Phase   `json:"phase,omitempty"`
}

// Map is one version of the shard map.
type Map struct {
	Version uint64              `json:"version"`
	Shards  map[ShardID]Shard   `json:"shards"`
	Tenants map[int64]Placement `json:"tenants"`
}

// Errors returned by lookups and updates.
var (
	ErrUnknownTenant   = errors.New("tenant is not in the shard map")
	ErrVersionConflict = errors.New("shard map changed since it was read")
	ErrInvalid         = errors.New("invalid shard map")
)

// Lookup returns the shard that currently serves a tenant, and its placement.
func (m Map) Lookup(tenant int64) (Shard, Placement, error) {
	p, ok := m.Tenants[tenant]
	if !ok {
		return Shard{}, Placement{}, fmt.Errorf("%w: %d", ErrUnknownTenant, tenant)
	}
	s, ok := m.Shards[p.Shard]
	if !ok {
		return Shard{}, Placement{}, fmt.Errorf("%w: tenant %d is on unknown shard %q", ErrInvalid, tenant, p.Shard)
	}
	return s, p, nil
}

// Validate checks that every placement names known shards and a sensible move.
func (m Map) Validate() error {
	for id, s := range m.Shards {
		if id == "" || s.ID != id || s.DSN == "" {
			return fmt.Errorf("%w: shard %q is incomplete", ErrInvalid, id)
		}
	}
	for t, p := range m.Tenants {
		if t <= 0 {
			return fmt.Errorf("%w: tenant ids are positive, got %d", ErrInvalid, t)
		}
		if _, ok := m.Shards[p.Shard]; !ok {
			return fmt.Errorf("%w: tenant %d is on unknown shard %q", ErrInvalid, t, p.Shard)
		}
		moving := p.MovingTo != ""
		if moving != (p.Phase != Stable) {
			return fmt.Errorf("%w: tenant %d has moving_to %q in phase %q", ErrInvalid, t, p.MovingTo, p.Phase)
		}
		if moving {
			if _, ok := m.Shards[p.MovingTo]; !ok || p.MovingTo == p.Shard {
				return fmt.Errorf("%w: tenant %d cannot move to %q", ErrInvalid, t, p.MovingTo)
			}
			if p.Phase != Copying && p.Phase != Replicating {
				return fmt.Errorf("%w: tenant %d has unknown phase %q", ErrInvalid, t, p.Phase)
			}
		}
	}
	return nil
}

// Clone returns a deep copy.
func (m Map) Clone() Map {
	return Map{Version: m.Version, Shards: maps.Clone(m.Shards), Tenants: maps.Clone(m.Tenants)}
}

// TenantsOn lists the tenants a shard serves, in order.
func (m Map) TenantsOn(shard ShardID) []int64 {
	var out []int64
	for t, p := range m.Tenants {
		if p.Shard == shard {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Store keeps the current map. CompareAndSwap writes next only if the stored version is still
// expect, and next.Version must be expect+1.
type Store interface {
	Load(ctx context.Context) (Map, error)
	CompareAndSwap(ctx context.Context, expect uint64, next Map) error
}

// Update applies fn to a copy of the current map and stores it as the next version, retrying a
// few times if another writer got there first. fn returning an error aborts without writing.
func Update(ctx context.Context, s Store, fn func(*Map) error) (Map, error) {
	for range 5 {
		cur, err := s.Load(ctx)
		if err != nil {
			return Map{}, err
		}
		next := cur.Clone()
		if next.Shards == nil {
			next.Shards = map[ShardID]Shard{}
		}
		if next.Tenants == nil {
			next.Tenants = map[int64]Placement{}
		}
		if err := fn(&next); err != nil {
			return Map{}, err
		}
		next.Version = cur.Version + 1
		if err := next.Validate(); err != nil {
			return Map{}, err
		}
		err = s.CompareAndSwap(ctx, cur.Version, next)
		if errors.Is(err, ErrVersionConflict) {
			continue
		}
		if err != nil {
			return Map{}, err
		}
		return next, nil
	}
	return Map{}, ErrVersionConflict
}

// MemoryStore keeps the map in memory (tests and single-process tools).
type MemoryStore struct {
	mu  sync.Mutex
	cur []byte
}

// Load returns the stored map (version 0 and empty when nothing was stored).
func (s *MemoryStore) Load(context.Context) (Map, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return Map{Shards: map[ShardID]Shard{}, Tenants: map[int64]Placement{}}, nil
	}
	var m Map
	return m, json.Unmarshal(s.cur, &m)
}

// CompareAndSwap stores next if the current version is expect.
func (s *MemoryStore) CompareAndSwap(_ context.Context, expect uint64, next Map) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur Map
	if s.cur != nil {
		if err := json.Unmarshal(s.cur, &cur); err != nil {
			return err
		}
	}
	if cur.Version != expect || next.Version != expect+1 {
		return ErrVersionConflict
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	s.cur = b
	return nil
}
