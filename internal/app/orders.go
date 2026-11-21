package app

import (
	"context"
	"fmt"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// Limites de pagination.
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// OrderService regroupe les cas d'usage des commandes.
type OrderService struct {
	uow   UnitOfWork
	now   func() time.Time
	newID func() order.ID
}

// NewOrderService assemble le service. now et newID sont des dépendances comme
// les autres : les tests les fixent.
func NewOrderService(uow UnitOfWork, now func() time.Time, newID func() order.ID) *OrderService {
	return &OrderService{uow: uow, now: now, newID: newID}
}

// PlaceOrderCommand est la demande de création d'une commande.
type PlaceOrderCommand struct {
	Customer string
	Lines    []order.Line
}

// PlaceOrder crée une commande, et enregistre l'événement qui l'annonce dans
// la même transaction.
func (s *OrderService) PlaceOrder(ctx context.Context, cmd PlaceOrderCommand) (order.Order, error) {
	o, event, err := order.Place(s.newID(), cmd.Customer, cmd.Lines, s.now())
	if err != nil {
		return order.Order{}, err
	}
	err = s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		if err := r.Orders.Create(ctx, o); err != nil {
			return err
		}
		return r.Outbox.Add(ctx, event)
	})
	if err != nil {
		return order.Order{}, fmt.Errorf("place order: %w", err)
	}
	return o, nil
}

// GetOrder lit une commande ; ErrNotFound si elle n'existe pas.
func (s *OrderService) GetOrder(ctx context.Context, id order.ID) (order.Order, error) {
	var o order.Order
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		o, err = r.Orders.Get(ctx, id)
		return err
	})
	return o, err
}

// Page est une page de commandes ; Next est le curseur de la suivante (zéro
// s'il n'y en a pas).
type Page struct {
	Orders []order.Order
	Next   order.ID
}

// ListOrders renvoie une page de commandes, de la plus récente à la plus
// ancienne.
func (s *OrderService) ListOrders(ctx context.Context, f ListFilter) (Page, error) {
	if f.Status != "" && !f.Status.Valid() {
		return Page{}, &order.ValidationError{Fields: []order.FieldError{{Field: "status", Message: "unknown status"}}}
	}
	if f.Limit <= 0 {
		f.Limit = DefaultPageSize
	}
	f.Limit = min(f.Limit, MaxPageSize)
	want := f.Limit
	f.Limit++ // un élément de plus pour savoir s'il existe une page suivante
	var orders []order.Order
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		orders, err = r.Orders.List(ctx, f)
		return err
	})
	if err != nil {
		return Page{}, err
	}
	p := Page{Orders: orders}
	if len(orders) > want {
		p.Orders = orders[:want]
		p.Next = orders[want-1].ID()
	}
	return p, nil
}

// CancelOrder annule une commande. Le verrou optimiste protège d'une
// modification concurrente : ErrConflict, l'appelant peut réessayer.
func (s *OrderService) CancelOrder(ctx context.Context, id order.ID, reason string) (order.Order, error) {
	var o order.Order
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		if o, err = r.Orders.Get(ctx, id); err != nil {
			return err
		}
		event, err := o.Cancel(reason, s.now())
		if err != nil {
			return err
		}
		if err := r.Orders.Update(ctx, o); err != nil {
			return err
		}
		return r.Outbox.Add(ctx, event)
	})
	if err != nil {
		return order.Order{}, fmt.Errorf("cancel order %s: %w", id, err)
	}
	return o, nil
}
