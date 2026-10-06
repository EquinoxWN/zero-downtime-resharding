package shardmap

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore keeps the map in one row of a control database, written with a version check.
type PGStore struct{ Pool *pgxpool.Pool }

// Init creates the table if needed.
func (s PGStore) Init(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS shard_map (
		id boolean PRIMARY KEY DEFAULT true CHECK (id),
		version bigint NOT NULL,
		body jsonb NOT NULL)`)
	return err
}

// Load reads the current map.
func (s PGStore) Load(ctx context.Context) (Map, error) {
	var body []byte
	err := s.Pool.QueryRow(ctx, `SELECT body FROM shard_map WHERE id`).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return Map{Shards: map[ShardID]Shard{}, Tenants: map[int64]Placement{}}, nil
	}
	if err != nil {
		return Map{}, err
	}
	var m Map
	return m, json.Unmarshal(body, &m)
}

// CompareAndSwap writes next only if the stored version is still expect.
func (s PGStore) CompareAndSwap(ctx context.Context, expect uint64, next Map) error {
	if next.Version != expect+1 {
		return ErrVersionConflict
	}
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	var tag interface{ RowsAffected() int64 }
	if expect == 0 {
		tag, err = s.Pool.Exec(ctx, `INSERT INTO shard_map (version, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`, int64(next.Version), body)
	} else {
		tag, err = s.Pool.Exec(ctx, `UPDATE shard_map SET version = $1, body = $2 WHERE id AND version = $3`, int64(next.Version), body, int64(expect))
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrVersionConflict
	}
	return nil
}
