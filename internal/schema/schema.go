// Package schema is the example application's tables. Every table leads its primary key with
// tenant_id: a tenant's rows can then be selected, filtered for replication and verified with
// an index, and PostgreSQL allows a row filter on tenant_id for updates and deletes because the
// column is part of the replica identity (the primary key).
package schema

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Table is one tenant-owned table.
type Table struct {
	Name string
	Key  string // primary key columns, used to order rows when checksumming
	DDL  string
}

// Tables lists the tenant-owned tables, parents first.
var Tables = []Table{
	{Name: "accounts", Key: "tenant_id, id", DDL: `CREATE TABLE IF NOT EXISTS accounts (
		tenant_id bigint NOT NULL,
		id bigint NOT NULL,
		email text NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (tenant_id, id))`},
	{Name: "orders", Key: "tenant_id, id", DDL: `CREATE TABLE IF NOT EXISTS orders (
		tenant_id bigint NOT NULL,
		id bigint NOT NULL,
		account_id bigint NOT NULL,
		total numeric(12, 2) NOT NULL,
		status text NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (tenant_id, id))`},
}

var ident = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// QuotedName is the table name as a safe SQL identifier; anything but a plain name is refused.
func (t Table) QuotedName() (string, error) {
	if !ident.MatchString(t.Name) {
		return "", fmt.Errorf("table name %q is not a plain identifier", t.Name)
	}
	return pgx.Identifier{t.Name}.Sanitize(), nil
}

// Execer is what both *pgx.Conn and *pgxpool.Pool provide.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Apply creates every table that does not exist yet.
func Apply(ctx context.Context, db Execer) error {
	for _, t := range Tables {
		if _, err := db.Exec(ctx, t.DDL); err != nil {
			return fmt.Errorf("create %s: %w", t.Name, err)
		}
	}
	return nil
}
