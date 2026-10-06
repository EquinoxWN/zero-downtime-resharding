package integration

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/EquinoxWN/zero-downtime-resharding/internal/move"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/router"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/shardmap"
	"github.com/EquinoxWN/zero-downtime-resharding/internal/verify"
)

func TestTheShardMapIsVersionedInTheControlDatabase(t *testing.T) {
	ctx := context.Background()
	before, err := env.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, err := shardmap.Update(ctx, env.store, func(m *shardmap.Map) error {
		m.Tenants[90] = shardmap.Placement{Shard: "shard-a"}
		return nil
	})
	if err != nil || after.Version != before.Version+1 {
		t.Fatalf("update: version %d -> %d, %v", before.Version, after.Version, err)
	}
	if err := env.store.CompareAndSwap(ctx, before.Version, after); !errors.Is(err, shardmap.ErrVersionConflict) {
		t.Fatalf("writing over a newer version must conflict: %v", err)
	}
	if got, _ := env.store.Load(ctx); got.Tenants[90].Shard != "shard-a" {
		t.Fatal("the update was not stored")
	}
}

func TestTheRouterSendsEachTenantToItsShard(t *testing.T) {
	ctx := context.Background()
	place(t, "shard-a", 101)
	place(t, "shard-b", 102)
	r, err := router.New(ctx, env.store, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, tenant := range []int64{101, 102} {
		err := r.Do(ctx, tenant, func(ctx context.Context, tx pgx.Tx) error {
			var seen string
			if err := tx.QueryRow(ctx, "SELECT current_setting('app.tenant_id')").Scan(&seen); err != nil {
				return err
			}
			if seen != map[int64]string{101: "101", 102: "102"}[tenant] {
				t.Errorf("transaction carries tenant %q", seen)
			}
			_, err := tx.Exec(ctx, "INSERT INTO accounts (tenant_id, id, email) VALUES ($1, 1, 'a@b.example')", tenant)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	a, b := shard(t, "shard-a"), shard(t, "shard-b")
	if count(t, a, "accounts", 101) != 1 || count(t, b, "accounts", 101) != 0 {
		t.Fatal("tenant 101 must only be on shard-a")
	}
	if count(t, b, "accounts", 102) != 1 || count(t, a, "accounts", 102) != 0 {
		t.Fatal("tenant 102 must only be on shard-b")
	}
	if err := r.Do(ctx, 0, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, router.ErrNoTenant) {
		t.Fatalf("a request without a tenant: %v", err)
	}
}

func TestAMoveCopiesExactlyOneTenantThenStreamsItsChanges(t *testing.T) {
	ctx := context.Background()
	a, b := shard(t, "shard-a"), shard(t, "shard-b")
	place(t, "shard-a", 201, 202)
	seed(t, a, 201, 50, 2000)
	seed(t, a, 202, 30, 500)
	mv := mover()
	if err := mv.Start(ctx, 201, "shard-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := mv.WaitCopied(ctx, 201, time.Minute); err != nil {
		t.Fatal(err)
	}
	m, _ := env.store.Load(ctx)
	if p := m.Tenants[201]; p.Shard != "shard-a" || p.MovingTo != "shard-b" || p.Phase != shardmap.Replicating {
		t.Fatalf("placement during the move: %+v", p)
	}
	if count(t, b, "orders", 201) != 2000 || count(t, b, "accounts", 201) != 50 {
		t.Fatal("the snapshot copy is incomplete")
	}
	if count(t, b, "orders", 202) != 0 || count(t, b, "accounts", 202) != 0 {
		t.Fatal("rows of another tenant were copied")
	}
	// Changes after the copy reach the target.
	for _, q := range []string{
		"INSERT INTO orders (tenant_id, id, account_id, total, status) VALUES (201, 5001, 1, 9.99, 'new')",
		"UPDATE orders SET status = 'paid', total = total + 1 WHERE tenant_id = 201 AND id <= 100",
		"DELETE FROM orders WHERE tenant_id = 201 AND id BETWEEN 101 AND 150",
		"UPDATE accounts SET email = 'changed@tenant201.example' WHERE tenant_id = 201 AND id = 7",
		"INSERT INTO orders (tenant_id, id, account_id, total, status) VALUES (202, 9001, 1, 1, 'other tenant')",
	} {
		if _, err := a.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mv.WaitCaughtUp(ctx, 201, 0, time.Minute); err != nil {
		t.Fatal(err)
	}
	report, err := verify.Tenant(ctx, a, b, 201)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range report {
		if !r.Equal() {
			t.Fatalf("%s differs: %d rows %s vs %d rows %s", r.Name, r.CountA, r.SumA, r.CountB, r.SumB)
		}
	}
	if count(t, b, "orders", 202) != 0 {
		t.Fatal("a write for another tenant was streamed to the target")
	}
	if err := mv.Abort(ctx, 201); err != nil {
		t.Fatal(err)
	}
}

func TestWritesDuringTheSnapshotCopyAreNeitherLostNorDuplicated(t *testing.T) {
	ctx := context.Background()
	a, b := shard(t, "shard-a"), shard(t, "shard-b")
	place(t, "shard-a", 301)
	seed(t, a, 301, 100, 20000)
	r, err := router.New(ctx, env.store, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var writes, failures atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			next := int64(100000 * (w + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := r.Do(ctx, 301, func(ctx context.Context, tx pgx.Tx) error {
					next++
					if _, err := tx.Exec(ctx, "INSERT INTO orders (tenant_id, id, account_id, total, status) VALUES (301, $1, 1, 5, 'live')", next); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, "UPDATE orders SET total = total + 1 WHERE tenant_id = 301 AND id = $1", 1+rng.Intn(20000))
					return err
				})
				if err != nil {
					failures.Add(1)
				} else {
					writes.Add(1)
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	mv := mover()
	if err := mv.Start(ctx, 301, "shard-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := mv.WaitCopied(ctx, 301, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if _, err := mv.WaitCaughtUp(ctx, 301, 0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if failures.Load() != 0 || writes.Load() < 50 {
		t.Fatalf("%d writes, %d failures: the source must keep serving during the copy", writes.Load(), failures.Load())
	}
	report, err := verify.Tenant(ctx, a, b, 301)
	if err != nil {
		t.Fatal(err)
	}
	for _, rep := range report {
		if !rep.Equal() {
			t.Fatalf("%s differs after %d concurrent writes: %d vs %d rows", rep.Name, writes.Load(), rep.CountA, rep.CountB)
		}
	}
	t.Logf("%d transactions during the move, 0 lost, checksums equal", writes.Load())
	if err := mv.Abort(ctx, 301); err != nil {
		t.Fatal(err)
	}
}

func TestMovesThatCannotWorkAreRefusedAndLeaveNothingBehind(t *testing.T) {
	ctx := context.Background()
	a, b := shard(t, "shard-a"), shard(t, "shard-b")
	place(t, "shard-a", 401, 402)
	seed(t, a, 401, 5, 10)
	seed(t, a, 402, 5, 10)
	mv := mover()
	if err := mv.Start(ctx, 499, "shard-b"); !errors.Is(err, shardmap.ErrUnknownTenant) {
		t.Fatalf("unknown tenant: %v", err)
	}
	if err := mv.Start(ctx, 401, "shard-a"); !errors.Is(err, move.ErrSameShard) {
		t.Fatalf("same shard: %v", err)
	}
	if err := mv.Start(ctx, 401, "shard-z"); !errors.Is(err, shardmap.ErrInvalid) {
		t.Fatalf("unknown shard: %v", err)
	}
	if err := mv.Start(ctx, 401, "shard-b"); err != nil {
		t.Fatal(err)
	}
	if err := mv.Start(ctx, 401, "shard-b"); !errors.Is(err, move.ErrAlreadyMoving) {
		t.Fatalf("second move of the same tenant: %v", err)
	}
	if err := mv.Abort(ctx, 401); err != nil {
		t.Fatal(err)
	}
	// Leftover rows on the target would merge two histories of the tenant: refuse.
	seed(t, b, 402, 1, 1)
	if err := mv.Start(ctx, 402, "shard-b"); !errors.Is(err, move.ErrTargetHasRows) {
		t.Fatalf("target with rows: %v", err)
	}
	m, _ := env.store.Load(ctx)
	if p := m.Tenants[402]; p.Phase != shardmap.Stable || p.Shard != "shard-a" {
		t.Fatalf("a refused move must leave the tenant stable: %+v", p)
	}
	var pubs int
	if err := a.QueryRow(ctx, "SELECT count(*) FROM pg_publication WHERE pubname = $1", move.Name(402)).Scan(&pubs); err != nil || pubs != 0 {
		t.Fatalf("a refused move left %d publications (%v)", pubs, err)
	}
}

func TestAbortRemovesTheSubscriptionSlotPublicationAndCopiedRows(t *testing.T) {
	ctx := context.Background()
	a, b := shard(t, "shard-a"), shard(t, "shard-b")
	place(t, "shard-a", 501)
	seed(t, a, 501, 10, 300)
	mv := mover()
	for round := range 2 {
		if err := mv.Start(ctx, 501, "shard-b"); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if _, err := mv.WaitCopied(ctx, 501, time.Minute); err != nil {
			t.Fatal(err)
		}
		if lag, err := mv.Lag(ctx, 501); err != nil || lag < 0 {
			t.Fatalf("lag %d %v", lag, err)
		}
		if err := mv.Abort(ctx, 501); err != nil {
			t.Fatal(err)
		}
		var subs, slots, pubs int
		_ = b.QueryRow(ctx, "SELECT count(*) FROM pg_subscription WHERE subname = $1", move.Name(501)).Scan(&subs)
		_ = a.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", move.Name(501)).Scan(&slots)
		_ = a.QueryRow(ctx, "SELECT count(*) FROM pg_publication WHERE pubname = $1", move.Name(501)).Scan(&pubs)
		if subs+slots+pubs != 0 {
			t.Fatalf("round %d left %d subscriptions, %d slots, %d publications", round, subs, slots, pubs)
		}
		if count(t, b, "orders", 501) != 0 || count(t, a, "orders", 501) != 300 {
			t.Fatal("abort must delete the copy and keep the source")
		}
		if _, err := mv.Lag(ctx, 501); !errors.Is(err, move.ErrNotMoving) {
			t.Fatalf("lag after abort: %v", err)
		}
	}
	waitFor(t, "map to show tenant 501 stable", func() bool {
		m, _ := env.store.Load(ctx)
		return m.Tenants[501].Phase == shardmap.Stable
	})
}
