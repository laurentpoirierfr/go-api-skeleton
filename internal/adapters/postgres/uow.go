package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres/db"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// OutboxFactory crée l'outbox d'une transaction : les événements sont écrits
// avec la même transaction pgx que les données. L'implémentation (Watermill)
// est fournie par l'assemblage, dans cmd/.
type OutboxFactory func(tx pgx.Tx) app.Outbox

// UnitOfWork exécute les cas d'usage dans des transactions PostgreSQL.
type UnitOfWork struct {
	pool   *pgxpool.Pool
	outbox OutboxFactory
}

// NewUnitOfWork assemble l'unité de travail. outbox peut être nil pour un
// binaire qui ne publie rien (le worker) : Outbox.Add renvoie alors une erreur.
func NewUnitOfWork(pool *pgxpool.Pool, outbox OutboxFactory) *UnitOfWork {
	return &UnitOfWork{pool: pool, outbox: outbox}
}

// Do exécute f dans une transaction : validée si f réussit, annulée sinon
// (et aussi si f panique).
func (u *UnitOfWork) Do(ctx context.Context, f func(ctx context.Context, r app.Repositories) error) error {
	return pgx.BeginTxFunc(ctx, u.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		q := db.New(tx)
		var outbox app.Outbox = noOutbox{}
		if u.outbox != nil {
			outbox = u.outbox(tx)
		}
		return f(ctx, app.Repositories{
			Orders:        orderRepository{q},
			Outbox:        outbox,
			Notifications: notificationRepository{q},
			Inbox:         inbox{q},
		})
	})
}

type noOutbox struct{}

func (noOutbox) Add(context.Context, ...order.Event) error {
	return errors.New("no outbox configured for this unit of work")
}
