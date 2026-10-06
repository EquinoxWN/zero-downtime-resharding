// Package move copies one tenant to another shard while it keeps taking writes: a publication
// on the source filtered to the tenant's rows, and a subscription on the target that first copies
// a consistent snapshot of those rows and then streams every later insert, update and delete.
// The cutover (a short write fence and the map flip) is M2.
package move

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/schema"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
)

// Errors a move can return before touching any database.
var (
	ErrAlreadyMoving = errors.New("tenant is already moving")
	ErrSameShard     = errors.New("tenant is already on that shard")
	ErrTargetHasRows = errors.New("target shard already has rows for this tenant")
	ErrNotMoving     = errors.New("tenant is not moving")
)

// Mover runs moves; Pool returns a connection pool for a shard.
type Mover struct {
	Store shardmap.Store
	Pool  func(ctx context.Context, s shardmap.Shard) (*pgxpool.Pool, error)
}

// Name is the publication, subscription and replication slot name for a tenant's move.
func Name(tenant int64) string { return fmt.Sprintf("zdr_move_%d", tenant) }

// PublicationSQL publishes exactly one tenant's rows of every table. The tenant id is an integer,
// so it is formatted, not interpolated from text.
func PublicationSQL(tenant int64) (string, error) {
	parts := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		q, err := t.QuotedName()
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s WHERE (tenant_id = %d)", q, tenant))
	}
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", Name(tenant), strings.Join(parts, ", ")), nil
}

// SubscriptionSQL subscribes the target to the source's publication; copy_data makes PostgreSQL
// copy a consistent snapshot first and stream from exactly where the snapshot ends.
func SubscriptionSQL(tenant int64, sourceDSN string) (string, error) {
	conninfo, err := Conninfo(sourceDSN)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = true)",
		Name(tenant), literal(conninfo), Name(tenant)), nil
}

// Conninfo turns a postgres:// URL into a libpq key=value string with every value quoted.
func Conninfo(dsn string) (string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("source shard DSN: %w", err)
	}
	q := func(v string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		q(cfg.Host), cfg.Port, q(cfg.Database), q(cfg.User), q(cfg.Password)), nil
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Start records the move in the shard map, then creates the publication and the subscription.
// The tenant keeps being served by its source shard throughout.
func (mv *Mover) Start(ctx context.Context, tenant int64, target shardmap.ShardID) error {
	m, err := shardmap.Update(ctx, mv.Store, func(m *shardmap.Map) error {
		p, ok := m.Tenants[tenant]
		switch {
		case !ok:
			return fmt.Errorf("%w: %d", shardmap.ErrUnknownTenant, tenant)
		case p.Phase != shardmap.Stable:
			return fmt.Errorf("%w: %d is %s to %s", ErrAlreadyMoving, tenant, p.Phase, p.MovingTo)
		case p.Shard == target:
			return fmt.Errorf("%w: %d is on %s", ErrSameShard, tenant, target)
		}
		m.Tenants[tenant] = shardmap.Placement{Shard: p.Shard, MovingTo: target, Phase: shardmap.Copying}
		return nil
	})
	if err != nil {
		return err
	}
	src, dst, err := mv.pools(ctx, m, tenant)
	if err != nil {
		return mv.rollback(ctx, tenant, err)
	}
	if err := schema.Apply(ctx, dst); err != nil {
		return mv.rollback(ctx, tenant, err)
	}
	for _, t := range schema.Tables {
		q, err := t.QuotedName()
		if err != nil {
			return mv.rollback(ctx, tenant, err)
		}
		var exists bool
		if err := dst.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE tenant_id = $1)", q), tenant).Scan(&exists); err != nil {
			return mv.rollback(ctx, tenant, err)
		}
		if exists {
			// Copying on top of existing rows would fail on duplicate keys, or worse, merge two
			// histories of the tenant. An operator has to clean the target first.
			return mv.rollback(ctx, tenant, fmt.Errorf("%w: %s", ErrTargetHasRows, t.Name))
		}
	}
	pub, err := PublicationSQL(tenant)
	if err != nil {
		return mv.rollback(ctx, tenant, err)
	}
	if _, err := src.Exec(ctx, pub); err != nil {
		return mv.rollback(ctx, tenant, fmt.Errorf("create publication: %w", err))
	}
	srcShard, _, _ := m.Lookup(tenant)
	sub, err := SubscriptionSQL(tenant, srcShard.DSN)
	if err != nil {
		return mv.cleanupAndRollback(ctx, tenant, err)
	}
	if _, err := dst.Exec(ctx, sub); err != nil {
		return mv.cleanupAndRollback(ctx, tenant, fmt.Errorf("create subscription: %w", err))
	}
	return nil
}

// WaitCopied waits until the snapshot copy of every table has finished and streaming has taken
// over, then records the replicating phase. It returns how long it waited.
func (mv *Mover) WaitCopied(ctx context.Context, tenant int64, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	m, err := mv.Store.Load(ctx)
	if err != nil {
		return 0, err
	}
	_, dst, err := mv.pools(ctx, m, tenant)
	if err != nil {
		return 0, err
	}
	deadline := start.Add(timeout)
	for {
		var total, ready int
		err := dst.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE r.srsubstate IN ('r', 's'))
			FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = $1`, Name(tenant)).Scan(&total, &ready)
		if err != nil {
			return 0, err
		}
		if total == len(schema.Tables) && ready == total {
			break
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("snapshot copy of tenant %d not finished after %v (%d of %d tables)", tenant, timeout, ready, total)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	_, err = shardmap.Update(ctx, mv.Store, func(m *shardmap.Map) error {
		p := m.Tenants[tenant]
		if p.Phase != shardmap.Copying {
			return fmt.Errorf("%w: %d", ErrNotMoving, tenant)
		}
		p.Phase = shardmap.Replicating
		m.Tenants[tenant] = p
		return nil
	})
	return time.Since(start), err
}

// Lag is how many bytes of the source's WAL the target has not yet confirmed.
func (mv *Mover) Lag(ctx context.Context, tenant int64) (int64, error) {
	m, err := mv.Store.Load(ctx)
	if err != nil {
		return 0, err
	}
	src, _, err := mv.pools(ctx, m, tenant)
	if err != nil {
		return 0, err
	}
	var lag int64
	err = src.QueryRow(ctx, `SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, Name(tenant)).Scan(&lag)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: no replication slot for tenant %d", ErrNotMoving, tenant)
	}
	return lag, err
}

// WaitCaughtUp waits until the lag is at most maxBytes.
func (mv *Mover) WaitCaughtUp(ctx context.Context, tenant int64, maxBytes int64, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		lag, err := mv.Lag(ctx, tenant)
		if err != nil || lag <= maxBytes {
			return lag, err
		}
		if time.Now().After(deadline) {
			return lag, fmt.Errorf("tenant %d still %d bytes behind after %v", tenant, lag, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Abort stops a move: drops the subscription (and with it the source's replication slot) and the
// publication, deletes the copied rows from the target, and marks the tenant stable on its source.
func (mv *Mover) Abort(ctx context.Context, tenant int64) error {
	m, err := mv.Store.Load(ctx)
	if err != nil {
		return err
	}
	if p, ok := m.Tenants[tenant]; !ok || p.Phase == shardmap.Stable {
		return fmt.Errorf("%w: %d", ErrNotMoving, tenant)
	}
	if err := mv.cleanup(ctx, m, tenant); err != nil {
		return err
	}
	_, err = shardmap.Update(ctx, mv.Store, func(m *shardmap.Map) error {
		p := m.Tenants[tenant]
		m.Tenants[tenant] = shardmap.Placement{Shard: p.Shard}
		return nil
	})
	return err
}

func (mv *Mover) cleanup(ctx context.Context, m shardmap.Map, tenant int64) error {
	src, dst, err := mv.pools(ctx, m, tenant)
	if err != nil {
		return err
	}
	if _, err := dst.Exec(ctx, "DROP SUBSCRIPTION IF EXISTS "+Name(tenant)); err != nil {
		return fmt.Errorf("drop subscription: %w", err)
	}
	if _, err := src.Exec(ctx, "DROP PUBLICATION IF EXISTS "+Name(tenant)); err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}
	for i := len(schema.Tables) - 1; i >= 0; i-- {
		q, err := schema.Tables[i].QuotedName()
		if err != nil {
			return err
		}
		if _, err := dst.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id = $1", q), tenant); err != nil && !isUndefinedTable(err) {
			return fmt.Errorf("delete copied rows: %w", err)
		}
	}
	return nil
}

func isUndefinedTable(err error) bool { return strings.Contains(err.Error(), "SQLSTATE 42P01") }

func (mv *Mover) pools(ctx context.Context, m shardmap.Map, tenant int64) (src, dst *pgxpool.Pool, err error) {
	p, ok := m.Tenants[tenant]
	if !ok || p.MovingTo == "" {
		return nil, nil, fmt.Errorf("%w: %d", ErrNotMoving, tenant)
	}
	if src, err = mv.Pool(ctx, m.Shards[p.Shard]); err != nil {
		return nil, nil, err
	}
	dst, err = mv.Pool(ctx, m.Shards[p.MovingTo])
	return src, dst, err
}

func (mv *Mover) cleanupAndRollback(ctx context.Context, tenant int64, cause error) error {
	if m, err := mv.Store.Load(ctx); err == nil {
		_ = mv.cleanup(ctx, m, tenant)
	}
	return mv.rollback(ctx, tenant, cause)
}

func (mv *Mover) rollback(ctx context.Context, tenant int64, cause error) error {
	_, err := shardmap.Update(ctx, mv.Store, func(m *shardmap.Map) error {
		p := m.Tenants[tenant]
		m.Tenants[tenant] = shardmap.Placement{Shard: p.Shard}
		return nil
	})
	return errors.Join(cause, err)
}
