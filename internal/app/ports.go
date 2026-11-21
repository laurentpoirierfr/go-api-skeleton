// Package app contient les cas d'usage, et les ports (interfaces) dont ils ont
// besoin. Il ne dépend que du domaine : les adaptateurs (PostgreSQL, mémoire,
// Watermill…) implémentent ces interfaces, et cmd/ les assemble.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// Erreurs des ports, reconnues par errors.Is.
var (
	ErrNotFound = errors.New("not found")
	// ErrConflict signale une modification concurrente : la version lue n'est
	// plus celle de la base (verrou optimiste).
	ErrConflict = errors.New("concurrent modification")
)

// OrderRepository stocke les commandes.
type OrderRepository interface {
	Create(ctx context.Context, o order.Order) error
	Get(ctx context.Context, id order.ID) (order.Order, error)
	List(ctx context.Context, f ListFilter) ([]order.Order, error)
	// Update enregistre une commande modifiée, si la base contient encore la
	// version précédente (o.Version()-1) ; sinon ErrConflict.
	Update(ctx context.Context, o order.Order) error
}

// ListFilter filtre et pagine une liste de commandes, de la plus récente à la
// plus ancienne.
type ListFilter struct {
	Status order.Status // vide : tous les statuts
	Before order.ID     // curseur : les commandes plus anciennes que celle-ci ; zéro : depuis le début
	Limit  int
}

// Outbox enregistre les événements à publier, dans la même transaction que
// les données : un événement n'est publié que si la donnée est enregistrée,
// et il n'est jamais perdu.
type Outbox interface {
	Add(ctx context.Context, events ...order.Event) error
}

// Notification est l'effet du worker : un message envoyé à un client.
type Notification struct {
	ID        string
	OrderID   order.ID
	Kind      string // le type de l'événement qui l'a produite
	Recipient string
	CreatedAt time.Time
}

// NotificationRepository stocke les notifications envoyées.
type NotificationRepository interface {
	Create(ctx context.Context, n Notification) error
	ListByOrder(ctx context.Context, id order.ID) ([]Notification, error)
}

// Inbox rend un consommateur idempotent : un message livré deux fois n'est
// traité qu'une fois.
type Inbox interface {
	// MarkProcessed enregistre le message, et renvoie faux s'il l'était déjà.
	MarkProcessed(ctx context.Context, consumer, messageID string) (bool, error)
}

// Repositories donne accès aux dépôts, tous dans la même transaction.
type Repositories struct {
	Orders        OrderRepository
	Outbox        Outbox
	Notifications NotificationRepository
	Inbox         Inbox
}

// UnitOfWork exécute f dans une transaction : tout ce que f écrit est
// enregistré si f réussit, et rien sinon.
type UnitOfWork interface {
	Do(ctx context.Context, f func(ctx context.Context, r Repositories) error) error
}
