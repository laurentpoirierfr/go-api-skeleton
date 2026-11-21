package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres/db"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

type orderRepository struct{ q *db.Queries }

func (r orderRepository) Create(ctx context.Context, o order.Order) error {
	s := o.Snapshot()
	if err := r.q.CreateOrder(ctx, db.CreateOrderParams{
		ID: uuid.UUID(s.ID), Customer: s.Customer, TotalCents: int64(s.Total), Status: string(s.Status),
		Version: int32(s.Version), CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}); err != nil {
		return fmt.Errorf("create order %s: %w", s.ID, err)
	}
	lines := make([]db.CreateOrderLinesParams, len(s.Lines))
	for i, l := range s.Lines {
		lines[i] = db.CreateOrderLinesParams{
			OrderID: uuid.UUID(s.ID), Position: int32(i), Product: l.Product,
			Quantity: int32(l.Quantity), UnitPriceCents: int64(l.UnitPrice),
		}
	}
	if _, err := r.q.CreateOrderLines(ctx, lines); err != nil {
		return fmt.Errorf("create lines of order %s: %w", s.ID, err)
	}
	return nil
}

func (r orderRepository) Get(ctx context.Context, id order.ID) (order.Order, error) {
	row, err := r.q.GetOrder(ctx, uuid.UUID(id))
	if errors.Is(err, pgx.ErrNoRows) {
		return order.Order{}, fmt.Errorf("order %s: %w", id, app.ErrNotFound)
	}
	if err != nil {
		return order.Order{}, fmt.Errorf("get order %s: %w", id, err)
	}
	orders, err := r.withLines(ctx, []db.Order{row})
	if err != nil {
		return order.Order{}, err
	}
	return orders[0], nil
}

func (r orderRepository) List(ctx context.Context, f app.ListFilter) ([]order.Order, error) {
	p := db.ListOrdersParams{RowLimit: int32(f.Limit)}
	if f.Status != "" {
		status := string(f.Status)
		p.Status = &status
	}
	if f.Before != (order.ID{}) {
		before := uuid.UUID(f.Before)
		p.Before = &before
	}
	rows, err := r.q.ListOrders(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	return r.withLines(ctx, rows)
}

// withLines charge les lignes de toutes les commandes en une seule requête.
func (r orderRepository) withLines(ctx context.Context, rows []db.Order) ([]order.Order, error) {
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	lineRows, err := r.q.ListOrderLines(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("list order lines: %w", err)
	}
	lines := make(map[uuid.UUID][]order.Line, len(rows))
	for _, l := range lineRows {
		lines[l.OrderID] = append(lines[l.OrderID], order.Line{
			Product: l.Product, Quantity: int(l.Quantity), UnitPrice: order.Money(l.UnitPriceCents),
		})
	}
	orders := make([]order.Order, len(rows))
	for i, row := range rows {
		orders[i] = order.Restore(order.Snapshot{
			ID: order.ID(row.ID), Customer: row.Customer, Lines: lines[row.ID], Total: order.Money(row.TotalCents),
			Status: order.Status(row.Status), Version: int(row.Version), CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		})
	}
	return orders, nil
}

func (r orderRepository) Update(ctx context.Context, o order.Order) error {
	s := o.Snapshot()
	n, err := r.q.UpdateOrder(ctx, db.UpdateOrderParams{
		ID: uuid.UUID(s.ID), Status: string(s.Status), Version: int32(s.Version),
		UpdatedAt: s.UpdatedAt, ExpectedVersion: int32(s.Version - 1),
	})
	if err != nil {
		return fmt.Errorf("update order %s: %w", s.ID, err)
	}
	if n == 1 {
		return nil
	}
	// Aucune ligne modifiée : la commande n'existe pas, ou sa version a changé.
	if _, err := r.q.GetOrder(ctx, uuid.UUID(s.ID)); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("order %s: %w", s.ID, app.ErrNotFound)
	}
	return fmt.Errorf("order %s: %w", s.ID, app.ErrConflict)
}

type notificationRepository struct{ q *db.Queries }

func (r notificationRepository) Create(ctx context.Context, n app.Notification) error {
	id, err := uuid.Parse(n.ID)
	if err != nil {
		return fmt.Errorf("notification ID %q: %w", n.ID, err)
	}
	return r.q.CreateNotification(ctx, db.CreateNotificationParams{
		ID: id, OrderID: uuid.UUID(n.OrderID), Kind: n.Kind, Recipient: n.Recipient, CreatedAt: n.CreatedAt,
	})
}

func (r notificationRepository) ListByOrder(ctx context.Context, id order.ID) ([]app.Notification, error) {
	rows, err := r.q.ListNotificationsByOrder(ctx, uuid.UUID(id))
	if err != nil {
		return nil, err
	}
	res := make([]app.Notification, len(rows))
	for i, row := range rows {
		res[i] = app.Notification{ID: row.ID.String(), OrderID: order.ID(row.OrderID), Kind: row.Kind, Recipient: row.Recipient, CreatedAt: row.CreatedAt}
	}
	return res, nil
}

type inbox struct{ q *db.Queries }

func (i inbox) MarkProcessed(ctx context.Context, consumer, messageID string) (bool, error) {
	n, err := i.q.MarkProcessed(ctx, db.MarkProcessedParams{Consumer: consumer, MessageID: messageID})
	return n == 1, err
}
