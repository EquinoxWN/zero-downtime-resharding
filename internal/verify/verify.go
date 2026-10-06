// Package verify compares one tenant's rows on two shards by count and checksum.
package verify

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/schema"
)

// Table is the comparison of one table.
type Table struct {
	Name           string
	CountA, CountB int64
	SumA, SumB     string
}

// Equal reports whether both sides hold the same rows.
func (t Table) Equal() bool { return t.CountA == t.CountB && t.SumA == t.SumB }

// Tenant computes, for every table, the tenant's row count and an md5 of its rows in key order on
// both shards.
func Tenant(ctx context.Context, a, b *pgxpool.Pool, tenant int64) ([]Table, error) {
	var out []Table
	for _, t := range schema.Tables {
		q, err := t.QuotedName()
		if err != nil {
			return nil, err
		}
		query := fmt.Sprintf(`SELECT count(*), COALESCE(md5(string_agg(x::text, '|' ORDER BY %s)), '')
			FROM %s x WHERE tenant_id = $1`, t.Key, q)
		r := Table{Name: t.Name}
		if err := a.QueryRow(ctx, query, tenant).Scan(&r.CountA, &r.SumA); err != nil {
			return nil, fmt.Errorf("%s on the first shard: %w", t.Name, err)
		}
		if err := b.QueryRow(ctx, query, tenant).Scan(&r.CountB, &r.SumB); err != nil {
			return nil, fmt.Errorf("%s on the second shard: %w", t.Name, err)
		}
		out = append(out, r)
	}
	return out, nil
}
