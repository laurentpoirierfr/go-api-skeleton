package order

import "time"

// Event est un fait métier, produit par une commande. Le type est le nom
// public de l'événement : il est versionné, et un changement incompatible
// crée une nouvelle version (order.created.v2) qui coexiste avec l'ancienne.
type Event interface {
	EventType() string
	AggregateID() ID
}

// OrderPlaced annonce une nouvelle commande.
type OrderPlaced struct {
	OrderID  ID
	Customer string
	Lines    []Line
	Total    Money
	PlacedAt time.Time
}

func (OrderPlaced) EventType() string { return "order.created.v1" }
func (e OrderPlaced) AggregateID() ID { return e.OrderID }

// OrderCancelled annonce l'annulation d'une commande.
type OrderCancelled struct {
	OrderID     ID
	Customer    string
	Reason      string
	CancelledAt time.Time
}

func (OrderCancelled) EventType() string { return "order.cancelled.v1" }
func (e OrderCancelled) AggregateID() ID { return e.OrderID }
