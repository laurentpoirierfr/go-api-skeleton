package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/config"
)

// Broker est ce qui distingue NATS JetStream de Redpanda : le reste de la
// messagerie (outbox, forwarder, consommateur, format des messages) est commun.
type Broker interface {
	// Name renvoie « nats » ou « redpanda ».
	Name() string
	// Publisher publie les messages (forwarder, DLQ). Il est utilisable même
	// si le broker est indisponible au démarrage : la publication échoue
	// alors, et sera réessayée.
	Publisher() message.Publisher
	// EnsureTopology crée ou met à jour ce dont les binaires ont besoin
	// (streams, topics, consommateurs durables) : idempotent.
	EnsureTopology(ctx context.Context, consumers ...ConsumerConfig) error
	// Subscriber crée l'abonnement d'un consommateur.
	Subscriber(c ConsumerConfig) (message.Subscriber, error)
	// SubscribeTopics renvoie les topics auxquels abonner un consommateur.
	SubscribeTopics() []string
	// DelaysRedelivery indique si le broker relivre lui-même un message en
	// échec après un délai, en comptant ses livraisons (JetStream). Sinon
	// (Kafka), le consommateur émule ce comportement.
	DelaysRedelivery() bool
	// Check vérifie la connexion, pour la sonde /health/ready.
	Check(ctx context.Context) error
	// Close ferme les connexions.
	Close() error
}

// DLQTopic renvoie le topic de la file des messages morts d'un consommateur.
func DLQTopic(consumer string) string { return "dlq." + consumer }

// WaitForTopology appelle EnsureTopology jusqu'à ce qu'il réussisse, ou que
// ctx soit annulé : un binaire peut démarrer avant le broker, ou pendant une
// coupure.
func WaitForTopology(ctx context.Context, b Broker, retry time.Duration, onError func(error), consumers ...ConsumerConfig) error {
	for {
		err := b.EnsureTopology(ctx, consumers...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		onError(err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// Options choisit et règle le broker.
type Options struct {
	Kind     string // nats ou redpanda
	NATSURL  string
	Redpanda RedpandaConfig
	Name     string // nom de la connexion (le service)
}

// OptionsFrom traduit la configuration des binaires.
func OptionsFrom(cfg config.Config) Options {
	r := cfg.Messaging.Redpanda
	return Options{
		Kind: cfg.Messaging.Broker, NATSURL: cfg.Messaging.NATS.URL, Name: cfg.Service.Name,
		Redpanda: RedpandaConfig{Brokers: r.Brokers, Partitions: r.Partitions, ReplicationFactor: r.ReplicationFactor},
	}
}

// NewBroker crée le broker choisi par la configuration.
func NewBroker(o Options, logger watermill.LoggerAdapter) (Broker, error) {
	switch o.Kind {
	case "nats":
		return NewNATS(o.NATSURL, o.Name, logger)
	case "redpanda":
		return NewRedpanda(o.Redpanda, logger)
	}
	return nil, fmt.Errorf("unknown broker %q", o.Kind)
}
