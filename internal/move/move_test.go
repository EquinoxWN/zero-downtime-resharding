package move

import (
	"strings"
	"testing"
)

func TestThePublicationIsFilteredToOneTenant(t *testing.T) {
	sql, err := PublicationSQL(42)
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE PUBLICATION zdr_move_42 FOR TABLE "accounts" WHERE (tenant_id = 42), "orders" WHERE (tenant_id = 42)`
	if sql != want {
		t.Fatalf("got  %s\nwant %s", sql, want)
	}
}

func TestConnectionDetailsAreQuotedTwice(t *testing.T) {
	ci, err := Conninfo(`postgres://app:p%27w%5Cd@localhost:5433/shard_a`)
	if err != nil {
		t.Fatal(err)
	}
	if want := `host='localhost' port=5433 dbname='shard_a' user='app' password='p\'w\\d'`; ci != want {
		t.Fatalf("conninfo %s, want %s", ci, want)
	}
	sql, err := SubscriptionSQL(7, `postgres://app:p%27w@localhost:5433/shard_a`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `password=''p\''w'''`) || !strings.HasPrefix(sql, "CREATE SUBSCRIPTION zdr_move_7 CONNECTION '") {
		t.Fatalf("the conninfo must be a single SQL literal: %s", sql)
	}
	if _, err := Conninfo("::not a url"); err == nil {
		t.Fatal("a bad DSN should be an error")
	}
}
