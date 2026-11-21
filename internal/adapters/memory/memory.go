// Package memory implémente les ports en mémoire : pour les tests des cas
// d'usage, et pour faire tourner l'application sans base. L'unité de travail
// est transactionnelle : f travaille sur une copie de l'état, gardée seulement
// si f réussit.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

type state struct {
	orders        map[order.ID]order.Snapshot
	outbox        []order.Event
	notifications []app.Notification
	inbox         map[string]bool
}

func (s state) clone() state {
	c := state{
		orders:        make(map[order.ID]order.Snapshot, len(s.orders)),
		outbox:        slices.Clone(s.outbox),
		notifications: slices.Clone(s.notifications),
		inbox:         maps.Clone(s.inbox),
	}
	for id, o := range s.orders {
		o.Lines = slices.Clone(o.Lines)
		c.orders[id] = o
	}
	return c
}

// Store est la base en mémoire. Les transactions sont exécutées l'une après
// l'autre : Do ne doit pas être appelée depuis une autre Do.
type Store struct {
	mu    sync.Mutex
	state state
}

// NewStore crée une base vide.
func NewStore() *Store {
	return &Store{state: state{orders: map[order.ID]order.Snapshot{}, inbox: map[string]bool{}}}
}

// Do exécute f dans une transaction.
func (s *Store) Do(ctx context.Context, f func(ctx context.Context, r app.Repositories) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := &txState{s.state.clone()}
	if err := f(ctx, app.Repositories{Orders: tx, Outbox: tx, Notifications: notifications{tx}, Inbox: tx}); err != nil {
		return err // la copie est abandonnée : rollback
	}
	s.state = tx.state
	return nil
}

// Events renvoie les événements enregistrés dans l'outbox (pour les tests).
func (s *Store) Events() []order.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.state.outbox)
}

type txState struct{ state }

func (t *txState) Create(_ context.Context, o order.Order) error {
	if _, exists := t.orders[o.ID()]; exists {
		return fmt.Errorf("order %s already exists", o.ID())
	}
	t.orders[o.ID()] = o.Snapshot()
	return nil
}

func (t *txState) Get(_ context.Context, id order.ID) (order.Order, error) {
	s, ok := t.orders[id]
	if !ok {
		return order.Order{}, fmt.Errorf("order %s: %w", id, app.ErrNotFound)
	}
	return order.Restore(s), nil
}

func (t *txState) List(_ context.Context, f app.ListFilter) ([]order.Order, error) {
	var res []order.Order
	for _, s := range t.orders {
		if f.Status != "" && s.Status != f.Status {
			continue
		}
		if f.Before != (order.ID{}) && compare(s.ID, f.Before) >= 0 {
			continue
		}
		res = append(res, order.Restore(s))
	}
	slices.SortFunc(res, func(a, b order.Order) int { return compare(b.ID(), a.ID()) }) // plus récentes d'abord
	if len(res) > f.Limit {
		res = res[:f.Limit]
	}
	return res, nil
}

func (t *txState) Update(_ context.Context, o order.Order) error {
	cur, ok := t.orders[o.ID()]
	if !ok {
		return fmt.Errorf("order %s: %w", o.ID(), app.ErrNotFound)
	}
	if cur.Version != o.Version()-1 {
		return fmt.Errorf("order %s: %w", o.ID(), app.ErrConflict)
	}
	t.orders[o.ID()] = o.Snapshot()
	return nil
}

func (t *txState) Add(_ context.Context, events ...order.Event) error {
	t.outbox = append(t.outbox, events...)
	return nil
}

func (t *txState) MarkProcessed(_ context.Context, consumer, messageID string) (bool, error) {
	key := consumer + "/" + messageID
	if t.inbox[key] {
		return false, nil
	}
	t.inbox[key] = true
	return true, nil
}

// notifications implémente NotificationRepository : ses méthodes portent les
// mêmes noms que celles du dépôt des commandes.
type notifications struct{ t *txState }

func (n notifications) Create(_ context.Context, notif app.Notification) error {
	n.t.notifications = append(n.t.notifications, notif)
	return nil
}

func (n notifications) ListByOrder(_ context.Context, id order.ID) ([]app.Notification, error) {
	var res []app.Notification
	for _, notif := range n.t.notifications {
		if notif.OrderID == id {
			res = append(res, notif)
		}
	}
	return res, nil
}

func compare(a, b order.ID) int { return bytes.Compare(a[:], b[:]) }
