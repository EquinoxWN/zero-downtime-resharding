// Package router sends each tenant's work to the shard that serves it, according to a cached
// copy of the versioned shard map.
package router

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
)

// ErrNoTenant is returned when a request has no tenant key.
var ErrNoTenant = errors.New("every request needs a tenant id (a positive integer)")

// Router routes by tenant. It is safe for concurrent use.
type Router struct {
	store  shardmap.Store
	maxAge time.Duration

	mu       sync.Mutex
	m        shardmap.Map
	loadedAt time.Time
	pools    map[shardmap.ShardID]*pgxpool.Pool
}

// New loads the map; maxAge bounds how long a cached map is trusted before it is re-read.
func New(ctx context.Context, store shardmap.Store, maxAge time.Duration) (*Router, error) {
	r := &Router{store: store, maxAge: maxAge, pools: map[shardmap.ShardID]*pgxpool.Pool{}}
	return r, r.Refresh(ctx)
}

// Refresh re-reads the shard map now.
func (r *Router) Refresh(ctx context.Context) error {
	m, err := r.store.Load(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.Version >= r.m.Version {
		r.m, r.loadedAt = m, time.Now()
	}
	return nil
}

// Version is the version of the cached map.
func (r *Router) Version() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m.Version
}

// Shard returns the shard serving tenant and the map version the answer came from.
func (r *Router) Shard(ctx context.Context, tenant int64) (shardmap.Shard, uint64, error) {
	if tenant <= 0 {
		return shardmap.Shard{}, 0, ErrNoTenant
	}
	r.mu.Lock()
	stale := time.Since(r.loadedAt) >= r.maxAge
	r.mu.Unlock()
	if stale {
		if err := r.Refresh(ctx); err != nil {
			return shardmap.Shard{}, 0, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, _, err := r.m.Lookup(tenant)
	if errors.Is(err, shardmap.ErrUnknownTenant) && !stale {
		// The tenant may have been added since the last refresh.
		r.mu.Unlock()
		refreshErr := r.Refresh(ctx)
		r.mu.Lock()
		if refreshErr != nil {
			return shardmap.Shard{}, 0, refreshErr
		}
		s, _, err = r.m.Lookup(tenant)
	}
	return s, r.m.Version, err
}

func (r *Router) pool(ctx context.Context, s shardmap.Shard) (*pgxpool.Pool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[s.ID]; ok {
		return p, nil
	}
	p, err := pgxpool.New(ctx, s.DSN)
	if err != nil {
		return nil, fmt.Errorf("connect to shard %s: %w", s.ID, err)
	}
	r.pools[s.ID] = p
	return p, nil
}

// Do runs fn in a transaction on the tenant's shard. The transaction carries the tenant id in
// the app.tenant_id setting, so row-level security policies can enforce it on every table.
func (r *Router) Do(ctx context.Context, tenant int64, fn func(ctx context.Context, tx pgx.Tx) error) error {
	s, _, err := r.Shard(ctx, tenant)
	if err != nil {
		return err
	}
	p, err := r.pool(ctx, s)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, fmt.Sprint(tenant)); err != nil {
			return err
		}
		return fn(ctx, tx)
	})
}

// Close releases every connection pool.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pools {
		p.Close()
	}
	r.pools = map[shardmap.ShardID]*pgxpool.Pool{}
}
