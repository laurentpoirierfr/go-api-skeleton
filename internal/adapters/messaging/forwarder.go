package messaging

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/runner"
)

// Forwarder relaie les événements de l'outbox vers out (NATS JetStream). Il
// tourne dans le binaire api ; plusieurs instances peuvent tourner ensemble :
// la lecture verrouille les lignes (FOR UPDATE), une seule publie à la fois,
// et l'ordre est conservé.
//
// Un message est supprimé de l'outbox après sa publication. Si l'instance
// s'arrête entre les deux, il est republié : JetStream élimine le doublon
// grâce à Nats-Msg-Id (fenêtre de déduplication du stream).
func Forwarder(pool *pgxpool.Pool, out message.Publisher, logger watermill.LoggerAdapter, pollInterval time.Duration) (runner.Component, error) {
	sub, err := sql.NewSubscriber(sql.BeginnerFromPgx(pool), sql.SubscriberConfig{
		SchemaAdapter:    queueSchema,
		OffsetsAdapter:   sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true},
		InitializeSchema: false,
		PollInterval:     pollInterval,
	}, logger)
	if err != nil {
		return runner.Component{}, err
	}
	fwd, err := forwarder.NewForwarder(sub, out, logger, forwarder.Config{ForwarderTopic: OutboxTopic})
	if err != nil {
		return runner.Component{}, err
	}
	return runner.Component{Name: "outbox forwarder", Run: func(ctx context.Context) error {
		// Run rend la main à l'annulation de ctx, après avoir arrêté le
		// routeur (et donc le traitement en cours).
		return fwd.Run(ctx)
	}}, nil
}
