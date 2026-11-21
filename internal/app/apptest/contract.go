// Package apptest contient les tests de contrat des ports : toute
// implémentation de l'unité de travail (mémoire, PostgreSQL) doit les passer.
package apptest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// NewUnitOfWork crée une unité de travail sur une base vide.
type NewUnitOfWork func(t *testing.T) app.UnitOfWork

var t0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func place(t *testing.T, customer string) order.Order {
	t.Helper()
	o, _, err := order.Place(order.NewID(), customer, []order.Line{{Product: "Gopher", Quantity: 3, UnitPrice: 1250}}, t0)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func do(t *testing.T, uow app.UnitOfWork, f func(context.Context, app.Repositories) error) {
	t.Helper()
	if err := uow.Do(context.Background(), f); err != nil {
		t.Fatal(err)
	}
}

// Run lance tous les tests de contrat.
func Run(t *testing.T, newUoW NewUnitOfWork) {
	t.Run("CreateAndGet", func(t *testing.T) {
		uow := newUoW(t)
		o := place(t, "ada@example.com")
		do(t, uow, func(ctx context.Context, r app.Repositories) error { return r.Orders.Create(ctx, o) })
		do(t, uow, func(ctx context.Context, r app.Repositories) error {
			got, err := r.Orders.Get(ctx, o.ID())
			if err != nil {
				return err
			}
			want := o.Snapshot()
			gs := got.Snapshot()
			if gs.ID != want.ID || gs.Customer != want.Customer || gs.Total != want.Total || gs.Status != want.Status ||
				gs.Version != want.Version || !gs.CreatedAt.Equal(want.CreatedAt) || !slices.Equal(gs.Lines, want.Lines) {
				t.Errorf("got %+v, want %+v", gs, want)
			}
			return nil
		})
	})

	t.Run("GetMissing", func(t *testing.T) {
		err := newUoW(t).Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
			_, err := r.Orders.Get(ctx, order.NewID())
			return err
		})
		if !errors.Is(err, app.ErrNotFound) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		uow := newUoW(t)
		o := place(t, "ada@example.com")
		do(t, uow, func(ctx context.Context, r app.Repositories) error { return r.Orders.Create(ctx, o) })
		if err := uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error { return r.Orders.Create(ctx, o) }); err == nil {
			t.Error("a duplicate must be refused")
		}
	})

	// Une erreur dans la transaction annule tout ce qu'elle a écrit.
	t.Run("Rollback", func(t *testing.T) {
		uow := newUoW(t)
		o := place(t, "ada@example.com")
		boom := errors.New("boom")
		err := uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
			if err := r.Orders.Create(ctx, o); err != nil {
				return err
			}
			if _, err := r.Inbox.MarkProcessed(ctx, "c", "m1"); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		do(t, uow, func(ctx context.Context, r app.Repositories) error {
			if _, err := r.Orders.Get(ctx, o.ID()); !errors.Is(err, app.ErrNotFound) {
				t.Errorf("order kept after rollback: %v", err)
			}
			if first, err := r.Inbox.MarkProcessed(ctx, "c", "m1"); err != nil || !first {
				t.Errorf("inbox kept after rollback: %v %v", first, err)
			}
			return nil
		})
	})

	// Le verrou optimiste : une mise à jour fondée sur une version périmée est
	// refusée.
	t.Run("OptimisticLock", func(t *testing.T) {
		uow := newUoW(t)
		o := place(t, "ada@example.com")
		do(t, uow, func(ctx context.Context, r app.Repositories) error { return r.Orders.Create(ctx, o) })
		first, second := o, o // deux lecteurs de la même version
		if _, err := first.Cancel("one", t0); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Cancel("two", t0); err != nil {
			t.Fatal(err)
		}
		do(t, uow, func(ctx context.Context, r app.Repositories) error { return r.Orders.Update(ctx, first) })
		err := uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error { return r.Orders.Update(ctx, second) })
		if !errors.Is(err, app.ErrConflict) {
			t.Errorf("err = %v", err)
		}
		do(t, uow, func(ctx context.Context, r app.Repositories) error {
			got, err := r.Orders.Get(ctx, o.ID())
			if err == nil && (got.Status() != order.StatusCancelled || got.Version() != 2) {
				t.Errorf("%+v", got.Snapshot())
			}
			return err
		})
	})

	t.Run("UpdateMissing", func(t *testing.T) {
		o := place(t, "ada@example.com")
		_, _ = o.Cancel("", t0)
		err := newUoW(t).Do(context.Background(), func(ctx context.Context, r app.Repositories) error { return r.Orders.Update(ctx, o) })
		if !errors.Is(err, app.ErrNotFound) {
			t.Errorf("err = %v", err)
		}
	})

	// La liste : plus récentes d'abord, filtre par statut, curseur, limite.
	t.Run("List", func(t *testing.T) {
		uow := newUoW(t)
		var ids []order.ID
		for i := range 5 {
			o := place(t, "ada@example.com")
			if i%2 == 1 {
				_, _ = o.Cancel("", t0)
			}
			ids = append(ids, o.ID())
			do(t, uow, func(ctx context.Context, r app.Repositories) error { return r.Orders.Create(ctx, o) })
		}
		list := func(f app.ListFilter) []order.ID {
			var res []order.ID
			do(t, uow, func(ctx context.Context, r app.Repositories) error {
				os, err := r.Orders.List(ctx, f)
				for _, o := range os {
					res = append(res, o.ID())
				}
				return err
			})
			return res
		}
		if got := list(app.ListFilter{Limit: 10}); !slices.Equal(got, []order.ID{ids[4], ids[3], ids[2], ids[1], ids[0]}) {
			t.Errorf("all: %v", got)
		}
		if got := list(app.ListFilter{Limit: 2}); !slices.Equal(got, []order.ID{ids[4], ids[3]}) {
			t.Errorf("limit: %v", got)
		}
		if got := list(app.ListFilter{Limit: 10, Before: ids[3]}); !slices.Equal(got, []order.ID{ids[2], ids[1], ids[0]}) {
			t.Errorf("before: %v", got)
		}
		if got := list(app.ListFilter{Limit: 10, Status: order.StatusCancelled}); !slices.Equal(got, []order.ID{ids[3], ids[1]}) {
			t.Errorf("status: %v", got)
		}
	})

	t.Run("Inbox", func(t *testing.T) {
		uow := newUoW(t)
		mark := func(consumer, id string) bool {
			var first bool
			do(t, uow, func(ctx context.Context, r app.Repositories) error {
				var err error
				first, err = r.Inbox.MarkProcessed(ctx, consumer, id)
				return err
			})
			return first
		}
		if !mark("notifications", "m1") || mark("notifications", "m1") || !mark("audit", "m1") {
			t.Error("a message is processed once per consumer")
		}
	})

	t.Run("Notifications", func(t *testing.T) {
		uow := newUoW(t)
		o := place(t, "ada@example.com")
		do(t, uow, func(ctx context.Context, r app.Repositories) error {
			if err := r.Orders.Create(ctx, o); err != nil {
				return err
			}
			return r.Notifications.Create(ctx, app.Notification{ID: order.NewID().String(), OrderID: o.ID(), Kind: "order.created.v1", Recipient: "ada@example.com", CreatedAt: t0})
		})
		do(t, uow, func(ctx context.Context, r app.Repositories) error {
			ns, err := r.Notifications.ListByOrder(ctx, o.ID())
			if err == nil && (len(ns) != 1 || ns[0].Kind != "order.created.v1" || ns[0].Recipient != "ada@example.com") {
				t.Errorf("%+v", ns)
			}
			return err
		})
	})
}
