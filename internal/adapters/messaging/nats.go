package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// La topologie JetStream : un stream pour tous les événements des commandes.
const (
	StreamName        = "ORDERS"
	StreamSubjects    = "orders.>"
	DuplicatesWindow  = 2 * time.Minute    // Nats-Msg-Id identiques éliminés pendant cette durée
	StreamMaxAge      = 7 * 24 * time.Hour // les événements sont gardés une semaine
	connectionTimeout = 5 * time.Second
)

// Connect ouvre la connexion à NATS. Elle se reconnecte d'elle-même : une
// coupure ne fait pas échouer le programme, et la sonde /health/ready la
// signale.
func Connect(url, name string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.Name(name),
		nats.Timeout(connectionTimeout),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)
}

// Check vérifie la connexion, pour la sonde /health/ready.
func Check(conn *nats.Conn) func(context.Context) error {
	return func(context.Context) error {
		if s := conn.Status(); s != nats.CONNECTED {
			return fmt.Errorf("NATS %s", s)
		}
		return nil
	}
}

// EnsureStream crée ou met à jour le stream des commandes : l'opération est
// idempotente, chaque binaire peut l'appeler au démarrage.
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
	return js, nil
}

// NewPublisher publie dans JetStream : sur le sujet égal au topic
// (orders.created…), avec accusé de réception du serveur, et l'en-tête
// Nats-Msg-Id égal à l'identifiant du message. Le stream est créé par
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

// WaitForStream appelle EnsureStream jusqu'à ce qu'il réussisse, ou que ctx
// soit annulé : un binaire peut démarrer avant NATS, ou pendant une coupure.
func WaitForStream(ctx context.Context, conn *nats.Conn, retry time.Duration, onError func(error)) (jetstream.JetStream, error) {
	for {
		js, err := EnsureStream(ctx, conn)
		if err == nil {
			return js, nil
		}
		onError(err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
}
