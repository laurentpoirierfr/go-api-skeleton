package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/memory"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

var now = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func clock() time.Time { return now }

var lines = []order.Line{{Product: "Gopher", Quantity: 2, UnitPrice: 1250}}

func TestPlaceOrder(t *testing.T) {
	store := memory.NewStore()
	s := app.NewOrderService(store, clock, order.NewID)
	o, err := s.PlaceOrder(context.Background(), app.PlaceOrderCommand{Customer: "ada@example.com", Lines: lines})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOrder(context.Background(), o.ID())
	if err != nil || got.Total() != 2500 {
		t.Errorf("%v %v", got.Snapshot(), err)
	}
	// L'événement est dans l'outbox, enregistré avec la commande.
	events := store.Events()
	if len(events) != 1 || events[0].EventType() != "order.created.v1" || events[0].AggregateID() != o.ID() {
		t.Errorf("outbox: %+v", events)
	}
}

// Une commande invalide n'écrit rien, ni commande ni événement.
func TestPlaceOrderInvalid(t *testing.T) {
	store := memory.NewStore()
	s := app.NewOrderService(store, clock, order.NewID)
	if _, err := s.PlaceOrder(context.Background(), app.PlaceOrderCommand{Customer: "nope"}); !errors.Is(err, order.ErrInvalid) {
		t.Errorf("err = %v", err)
	}
	if len(store.Events()) != 0 {
		t.Error("an event was recorded for an invalid order")
	}
}

func TestCancelOrder(t *testing.T) {
	store := memory.NewStore()
	s := app.NewOrderService(store, clock, order.NewID)
	o, _ := s.PlaceOrder(context.Background(), app.PlaceOrderCommand{Customer: "ada@example.com", Lines: lines})
	c, err := s.CancelOrder(context.Background(), o.ID(), "too late")
	if err != nil || c.Status() != order.StatusCancelled || c.Version() != 2 {
		t.Fatalf("%v %v", c.Snapshot(), err)
	}
	if _, err := s.CancelOrder(context.Background(), o.ID(), "again"); !errors.Is(err, order.ErrAlreadyCancelled) {
		t.Errorf("second cancel: %v", err)
	}
	if _, err := s.CancelOrder(context.Background(), order.NewID(), ""); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	// Deux événements : la création et une seule annulation.
	if events := store.Events(); len(events) != 2 || events[1].EventType() != "order.cancelled.v1" {
		t.Errorf("outbox: %+v", events)
	}
}

func TestListOrders(t *testing.T) {
	s := app.NewOrderService(memory.NewStore(), clock, order.NewID)
	for range 5 {
		if _, err := s.PlaceOrder(context.Background(), app.PlaceOrderCommand{Customer: "ada@example.com", Lines: lines}); err != nil {
			t.Fatal(err)
		}
	}
	p1, err := s.ListOrders(context.Background(), app.ListFilter{Limit: 2})
	if err != nil || len(p1.Orders) != 2 || p1.Next == (order.ID{}) {
		t.Fatalf("%+v %v", p1, err)
	}
	p2, _ := s.ListOrders(context.Background(), app.ListFilter{Limit: 2, Before: p1.Next})
	p3, _ := s.ListOrders(context.Background(), app.ListFilter{Limit: 2, Before: p2.Next})
	if len(p2.Orders) != 2 || len(p3.Orders) != 1 || p3.Next != (order.ID{}) {
		t.Errorf("pages: %d %d, last next %v", len(p2.Orders), len(p3.Orders), p3.Next)
	}
	if _, err := s.ListOrders(context.Background(), app.ListFilter{Status: "shipped"}); !errors.Is(err, order.ErrInvalid) {
		t.Errorf("unknown status: %v", err)
	}
	if p, _ := s.ListOrders(context.Background(), app.ListFilter{Limit: 1000}); len(p.Orders) != 5 {
		t.Errorf("limit capped: %d", len(p.Orders))
	}
}

// Un événement livré deux fois ne produit qu'une notification.
func TestNotificationIdempotent(t *testing.T) {
	store := memory.NewStore()
	orders := app.NewOrderService(store, clock, order.NewID)
	o, _ := orders.PlaceOrder(context.Background(), app.PlaceOrderCommand{Customer: "ada@example.com", Lines: lines})
	notifications := app.NewNotificationService(store, clock, func() string { return order.NewID().String() })
	event := store.Events()[0]
	for i, want := range []bool{true, false} {
		created, err := notifications.Handle(context.Background(), "msg-1", event)
		if err != nil || created != want {
			t.Errorf("delivery %d: created %v, err %v", i+1, created, err)
		}
	}
	var count int
	_ = store.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
		ns, err := r.Notifications.ListByOrder(ctx, o.ID())
		count = len(ns)
		return err
	})
	if count != 1 {
		t.Errorf("%d notifications, want 1", count)
	}
}
