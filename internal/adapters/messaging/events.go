// Package messaging relie l'application à la messagerie avec Watermill :
//
//   - events.go : le format des messages (CloudEvents, mode binaire) ;
//   - outbox.go : l'outbox, écrite dans la transaction des données ;
//   - forwarder.go : le relais de l'outbox vers NATS JetStream ;
//   - nats.go : la connexion et la topologie JetStream.
package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// Les sujets NATS (topics Watermill) des événements. La version de
// l'événement est dans son type (ce-type), pas dans le sujet : une v2 peut
// coexister avec la v1 sur le même sujet pendant une migration.
const (
	TopicOrderCreated   = "orders.created"
	TopicOrderCancelled = "orders.cancelled"
	Source              = "go-api-skeleton/orders"
)

// Les attributs CloudEvents, en mode binaire : dans les métadonnées du
// message (les en-têtes NATS), le corps ne portant que les données.
const (
	HeaderSpecVersion = "ce-specversion"
	HeaderID          = "ce-id"
	HeaderType        = "ce-type"
	HeaderSource      = "ce-source"
	HeaderSubject     = "ce-subject"
	HeaderTime        = "ce-time"
	HeaderContentType = "content-type"
)

// Les données publiées sont un contrat public, distinct des types du domaine :
// le domaine peut évoluer sans casser les consommateurs.
type lineV1 struct {
	Product        string `json:"product"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type orderCreatedV1 struct {
	OrderID    string    `json:"order_id"`
	Customer   string    `json:"customer"`
	TotalCents int64     `json:"total_cents"`
	Lines      []lineV1  `json:"lines"`
	PlacedAt   time.Time `json:"placed_at"`
}

type orderCancelledV1 struct {
	OrderID     string    `json:"order_id"`
	Customer    string    `json:"customer"`
	Reason      string    `json:"reason,omitempty"`
	CancelledAt time.Time `json:"cancelled_at"`
}

// Encode transforme un événement en message Watermill, et renvoie son sujet.
// id identifie le message : il devient ce-id, et l'en-tête Nats-Msg-Id qui
// permet à JetStream d'éliminer les doublons.
func Encode(e order.Event, id string, now time.Time) (string, *message.Message, error) {
	var (
		topic string
		data  any
	)
	switch ev := e.(type) {
	case order.OrderPlaced:
		lines := make([]lineV1, len(ev.Lines))
		for i, l := range ev.Lines {
			lines[i] = lineV1{Product: l.Product, Quantity: l.Quantity, UnitPriceCents: int64(l.UnitPrice)}
		}
		topic, data = TopicOrderCreated, orderCreatedV1{
			OrderID: ev.OrderID.String(), Customer: ev.Customer, TotalCents: int64(ev.Total), Lines: lines, PlacedAt: ev.PlacedAt,
		}
	case order.OrderCancelled:
		topic, data = TopicOrderCancelled, orderCancelledV1{
			OrderID: ev.OrderID.String(), Customer: ev.Customer, Reason: ev.Reason, CancelledAt: ev.CancelledAt,
		}
	default:
		return "", nil, fmt.Errorf("unknown event %T", e)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return "", nil, err
	}
	msg := message.NewMessage(id, payload)
	msg.Metadata.Set(HeaderSpecVersion, "1.0")
	msg.Metadata.Set(HeaderID, id)
	msg.Metadata.Set(HeaderType, e.EventType())
	msg.Metadata.Set(HeaderSource, Source)
	msg.Metadata.Set(HeaderSubject, e.AggregateID().String())
	msg.Metadata.Set(HeaderTime, now.UTC().Format(time.RFC3339Nano))
	msg.Metadata.Set(HeaderContentType, "application/json")
	return topic, msg, nil
}

// Decode reconstruit l'événement d'un message reçu. Un type inconnu est une
// erreur : le consommateur ne sait pas le traiter.
func Decode(msg *message.Message) (order.Event, error) {
	switch t := msg.Metadata.Get(HeaderType); t {
	case order.OrderPlaced{}.EventType():
		var d orderCreatedV1
		if err := json.Unmarshal(msg.Payload, &d); err != nil {
			return nil, fmt.Errorf("decode %s: %w", t, err)
		}
		id, err := order.ParseID(d.OrderID)
		if err != nil {
			return nil, err
		}
		lines := make([]order.Line, len(d.Lines))
		for i, l := range d.Lines {
			lines[i] = order.Line{Product: l.Product, Quantity: l.Quantity, UnitPrice: order.Money(l.UnitPriceCents)}
		}
		return order.OrderPlaced{OrderID: id, Customer: d.Customer, Lines: lines, Total: order.Money(d.TotalCents), PlacedAt: d.PlacedAt}, nil
	case order.OrderCancelled{}.EventType():
		var d orderCancelledV1
		if err := json.Unmarshal(msg.Payload, &d); err != nil {
			return nil, fmt.Errorf("decode %s: %w", t, err)
		}
		id, err := order.ParseID(d.OrderID)
		if err != nil {
			return nil, err
		}
		return order.OrderCancelled{OrderID: id, Customer: d.Customer, Reason: d.Reason, CancelledAt: d.CancelledAt}, nil
	default:
		return nil, fmt.Errorf("unknown event type %q", t)
	}
}
