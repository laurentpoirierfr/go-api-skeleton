package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// OutboxTopic est le topic SQL de l'outbox : la table watermill_outbox
// (migration 000002).
const OutboxTopic = "outbox"

// queueSchema est le schéma « file » de watermill-sql : les messages non
// acquittés sont lus dans l'ordre, puis supprimés une fois publiés.
var queueSchema = sql.PostgreSQLQueueSchema{}

// Outbox écrit les événements dans la table de l'outbox, avec la transaction
// des données. Le forwarder les relaie ensuite vers NATS JetStream.
type Outbox struct {
	pub message.Publisher
	now func() time.Time
}

// NewOutbox crée l'outbox d'une transaction : à brancher sur
// postgres.NewUnitOfWork.
func NewOutbox(tx pgx.Tx, logger watermill.LoggerAdapter) app.Outbox {
	sqlPub, err := sql.NewPublisher(sql.TxFromPgx(tx), sql.PublisherConfig{
		SchemaAdapter:        queueSchema,
		AutoInitializeSchema: false, // la table est créée par les migrations, jamais dans une transaction
	}, logger)
	if err != nil {
		return failingOutbox{err}
	}
	// Le publieur du forwarder enveloppe chaque message avec son sujet de
	// destination, et l'écrit dans le topic de l'outbox.
	return Outbox{
		pub: forwarder.NewPublisher(sqlPub, forwarder.PublisherConfig{ForwarderTopic: OutboxTopic}),
		now: time.Now,
	}
}

// Add enregistre les événements dans la transaction en cours.
func (o Outbox) Add(ctx context.Context, events ...order.Event) error {
	for _, e := range events {
		topic, msg, err := Encode(e, uuid.Must(uuid.NewV7()).String(), o.now())
		if err != nil {
			return err
		}
		msg.SetContext(ctx)
		if err := o.pub.Publish(topic, msg); err != nil {
			return fmt.Errorf("outbox %s: %w", e.EventType(), err)
		}
	}
	return nil
}

type failingOutbox struct{ err error }

func (f failingOutbox) Add(context.Context, ...order.Event) error { return f.err }
