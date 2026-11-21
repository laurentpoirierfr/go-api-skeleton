package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// La topologie JetStream : un stream pour tous les événements des commandes,
// et un stream pour la file des messages morts (DLQ) de tous les
// consommateurs.
const (
	StreamName        = "ORDERS"
	StreamSubjects    = "orders.>"
	DLQStreamName     = "DLQ"
	DLQSubjects       = "dlq.>"
	DLQMaxAge         = 30 * 24 * time.Hour // le temps de les examiner
	DuplicatesWindow  = 2 * time.Minute     // Nats-Msg-Id identiques éliminés pendant cette durée
	StreamMaxAge      = 7 * 24 * time.Hour  // les événements sont gardés une semaine
	connectionTimeout = 5 * time.Second
)

// NATS est le broker NATS JetStream.
type NATS struct {
	conn   *nats.Conn
	pub    message.Publisher
	logger watermill.LoggerAdapter
}

// NewNATS se connecte à NATS. La connexion se rétablit d'elle-même : une
// coupure ne fait pas échouer le programme, et la sonde la signale.
func NewNATS(url, name string, logger watermill.LoggerAdapter) (*NATS, error) {
	conn, err := Connect(url, name)
	if err != nil {
		return nil, err
	}
	pub, err := NewPublisher(conn, logger)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &NATS{conn: conn, pub: pub, logger: logger}, nil
}

func (n *NATS) Name() string                    { return "nats" }
func (n *NATS) Publisher() message.Publisher    { return n.pub }
func (n *NATS) SubscribeTopics() []string       { return []string{StreamSubjects} }
func (n *NATS) DelaysRedelivery() bool          { return true }
func (n *NATS) Close() error                    { n.conn.Close(); return nil }
func (n *NATS) Check(ctx context.Context) error { return Check(n.conn)(ctx) }

// Conn renvoie la connexion (pour les tests).
func (n *NATS) Conn() *nats.Conn { return n.conn }

// EnsureTopology crée les streams, puis les consommateurs durables.
func (n *NATS) EnsureTopology(ctx context.Context, consumers ...ConsumerConfig) error {
	js, err := EnsureStream(ctx, n.conn)
	if err != nil {
		return err
	}
	for _, c := range consumers {
		if err := EnsureConsumer(ctx, js, c); err != nil {
			return err
		}
	}
	return nil
}

// Subscriber rattache Watermill au consommateur durable créé par
// EnsureTopology. Il a sa propre connexion : à sa fermeture, Watermill draine
// la connexion qu'il utilise, ce qui couperait une connexion partagée.
func (n *NATS) Subscriber(c ConsumerConfig) (message.Subscriber, error) {
	subConn, err := Connect(strings.Join(n.conn.Servers(), ","), n.conn.Opts.Name+"-"+c.Name)
	if err != nil {
		return nil, err
	}
	return wmjs.NewSubscriber(wmjs.SubscriberConfig{
		Conn:           subConn,
		Logger:         n.logger,
		AckWaitTimeout: c.AckWait,
		ResourceInitializer: func(ctx context.Context, js jetstream.JetStream, _ string) (jetstream.Consumer, func(context.Context, watermill.LoggerAdapter), error) {
			cons, err := js.Consumer(ctx, StreamName, c.Name)
			return cons, nil, err
		},
		// JetStream relivre après ce délai, et compte les livraisons.
		NakDelay:    backoff(c.Backoff),
		Unmarshaler: deliveryCountUnmarshaler{},
	})
}

// Connect ouvre une connexion à NATS, qui se rétablit d'elle-même.
func Connect(url, name string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.Name(name),
		nats.Timeout(connectionTimeout),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)
}

// Check vérifie la connexion.
func Check(conn *nats.Conn) func(context.Context) error {
	return func(context.Context) error {
		if s := conn.Status(); s != nats.CONNECTED {
			return fmt.Errorf("NATS %s", s)
		}
		return nil
	}
}

// EnsureStream crée ou met à jour le stream des commandes et celui de la DLQ.
func EnsureStream(ctx context.Context, conn *nats.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{StreamSubjects},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy, // plusieurs consommateurs lisent le même stream
		MaxAge:     StreamMaxAge,
		Duplicates: DuplicatesWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", StreamName, err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       DLQStreamName,
		Subjects:   []string{DLQSubjects},
		Storage:    jetstream.FileStorage,
		MaxAge:     DLQMaxAge,
		Duplicates: DuplicatesWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", DLQStreamName, err)
	}
	return js, nil
}

// EnsureConsumer crée ou met à jour un consommateur durable : il reprend où
// il s'était arrêté, même après un redémarrage du worker.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig) error {
	_, err := js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:        cfg.Name,
		FilterSubjects: []string{StreamSubjects},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        cfg.AckWait,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		// Illimité : c'est le consommateur qui décide d'envoyer en DLQ, aucun
		// message n'est abandonné en silence.
		MaxDeliver:    -1,
		MaxAckPending: 100,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", cfg.Name, err)
	}
	return nil
}

// NewPublisher publie dans JetStream : sur le sujet égal au topic
// (orders.created…), avec accusé de réception du serveur, et l'en-tête
// Nats-Msg-Id égal à l'identifiant du message. Les streams sont créés par
// EnsureStream, jamais à la volée.
func NewPublisher(conn *nats.Conn, logger watermill.LoggerAdapter) (*wmnats.Publisher, error) {
	if conn == nil {
		return nil, errors.New("nil NATS connection")
	}
	return wmnats.NewPublisherWithNatsConn(conn, wmnats.PublisherPublishConfig{
		Marshaler: &wmnats.NATSMarshaler{}, // métadonnées en en-têtes NATS, corps brut
		// Obligatoire même sans création de stream : le sujet est le topic.
		SubjectCalculator: wmnats.DefaultSubjectCalculator,
		JetStream: wmnats.JetStreamConfig{
			AutoProvision: false,
			TrackMsgID:    true,
		},
	}, logger)
}
