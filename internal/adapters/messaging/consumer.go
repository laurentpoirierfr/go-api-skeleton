package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/runner"
)

// EventHandler traite un événement reçu, et renvoie vrai s'il a produit un
// effet (faux pour un doublon). app.NotificationService le satisfait.
type EventHandler interface {
	Handle(ctx context.Context, messageID string, e order.Event) (bool, error)
}

// ConsumerConfig règle un consommateur.
type ConsumerConfig struct {
	// Name est le nom du consommateur durable JetStream, et celui du handler.
	Name string
	// MaxDeliveries : au-delà, un message toujours en échec part en DLQ.
	MaxDeliveries int
	// AckWait : sans acquittement dans ce délai, JetStream relivre le message.
	AckWait time.Duration
	// Retries et RetryInterval : les essais rapides, dans le processus, avant
	// de rendre le message à JetStream (pour les coupures très brèves).
	Retries       int
	RetryInterval time.Duration
	// Backoff : le délai avant la livraison suivante, selon le numéro de la
	// livraison ; le dernier s'applique ensuite.
	Backoff []time.Duration
}

// DeliveryCountKey est la métadonnée qui porte le numéro de livraison d'un
// message (1 à la première).
const DeliveryCountKey = "delivery_count"

// Les deux sortes d'échec : une erreur définitive (message illisible, type
// inconnu) ne se réessaie pas ; une erreur en DLQ quitte le circuit.
var (
	errPermanent  = errors.New("permanent failure")
	errDeadLetter = errors.New("dead letter")
)

// EnsureConsumer crée ou met à jour le consommateur durable : il reprend où
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

// Consumer fait du traitement des événements un composant : il attend le
// stream, crée le consommateur durable, puis fait tourner le Router
// Watermill jusqu'à l'annulation du contexte.
//
// conn sert à la topologie et à la DLQ. Le subscriber a sa propre connexion,
// ouverte vers les mêmes serveurs : à sa fermeture, Watermill draine la
// connexion qu'il utilise, ce qui couperait une connexion partagée.
func Consumer(conn *nats.Conn, h EventHandler, log *slog.Logger, wl watermill.LoggerAdapter, cfg ConsumerConfig) (runner.Component, error) {
	subConn, err := Connect(strings.Join(conn.Servers(), ","), conn.Opts.Name+"-"+cfg.Name)
	if err != nil {
		return runner.Component{}, err
	}
	sub, err := wmjs.NewSubscriber(wmjs.SubscriberConfig{
		Conn:           subConn,
		Logger:         wl,
		AckWaitTimeout: cfg.AckWait,
		// Le consommateur est le nôtre (EnsureConsumer) : Watermill s'y
		// rattache, sans créer de stream ni de consommateur à partir du topic.
		ResourceInitializer: func(ctx context.Context, js jetstream.JetStream, _ string) (jetstream.Consumer, func(context.Context, watermill.LoggerAdapter), error) {
			c, err := js.Consumer(ctx, StreamName, cfg.Name)
			return c, nil, err
		},
		NakDelay:    backoff(cfg.Backoff),
		Unmarshaler: deliveryCountUnmarshaler{},
	})
	if err != nil {
		return runner.Component{}, err
	}
	dlq, err := NewPublisher(conn, wl)
	if err != nil {
		return runner.Component{}, err
	}
	router, err := message.NewRouter(message.RouterConfig{CloseTimeout: cfg.AckWait}, wl)
	if err != nil {
		return runner.Component{}, err
	}
	// Le premier middleware est le plus extérieur : la DLQ reçoit ce que les
	// essais n'ont pas résolu ; Recoverer change une panique en erreur.
	poison, err := middleware.PoisonQueueWithFilter(dlq, "dlq."+cfg.Name, func(err error) bool {
		return errors.Is(err, errDeadLetter)
	})
	if err != nil {
		return runner.Component{}, err
	}
	router.AddMiddleware(
		poison,
		middleware.Retry{
			MaxRetries:      cfg.Retries,
			InitialInterval: cfg.RetryInterval,
			Multiplier:      2,
			Logger:          wl,
			ShouldRetry:     func(p middleware.RetryParams) bool { return !errors.Is(p.Err, errPermanent) },
		}.Middleware,
		middleware.Recoverer,
	)
	router.AddConsumerHandler(cfg.Name, StreamSubjects, sub, handle(h, log, cfg.MaxDeliveries))

	return runner.Component{Name: "consumer " + cfg.Name, Run: func(ctx context.Context) error {
		js, err := WaitForStream(ctx, conn, 2*time.Second, func(err error) {
			log.WarnContext(ctx, "waiting for the NATS stream", "err", err)
		})
		if err != nil {
			return nil // annulé pendant l'attente : arrêt normal
		}
		if err := EnsureConsumer(ctx, js, cfg); err != nil {
			if ctx.Err() != nil {
				return nil // annulé pendant la création : arrêt normal
			}
			return err
		}
		return router.Run(ctx)
	}}, nil
}

// handle traduit un message en appel du cas d'usage, et classe les échecs.
func handle(h EventHandler, log *slog.Logger, maxDeliveries int) message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		ctx := msg.Context()
		deliveries, _ := strconv.Atoi(msg.Metadata.Get(DeliveryCountKey))
		event, err := Decode(msg)
		if err != nil {
			return fmt.Errorf("%w: %w: %w", errDeadLetter, errPermanent, err)
		}
		created, err := h.Handle(ctx, msg.UUID, event)
		if err != nil {
			if deliveries >= maxDeliveries {
				return fmt.Errorf("%w after %d deliveries: %w", errDeadLetter, deliveries, err)
			}
			return err // Nak : JetStream relivrera après le délai de backoff
		}
		log.InfoContext(ctx, "event handled", "type", event.EventType(), "message_id", msg.UUID,
			"order_id", event.AggregateID().String(), "duplicate", !created, "delivery", deliveries)
		return nil
	}
}

// deliveryCountUnmarshaler ajoute aux métadonnées le numéro de livraison du
// message, que le décodeur par défaut ne transmet pas.
type deliveryCountUnmarshaler struct{ wmjs.DefaultUnmarshaler }

func (u deliveryCountUnmarshaler) Unmarshal(m jetstream.Msg) (*message.Message, error) {
	msg, err := u.DefaultUnmarshaler.Unmarshal(m)
	if err != nil {
		return nil, err
	}
	if md, err := m.Metadata(); err == nil {
		msg.Metadata.Set(DeliveryCountKey, strconv.FormatUint(md.NumDelivered, 10))
	}
	return msg, nil
}

// backoff donne le délai avant la livraison suivante.
type backoff []time.Duration

func (b backoff) WaitTime(delivery uint64) time.Duration {
	if len(b) == 0 {
		return 0
	}
	i := int(min(delivery, uint64(len(b)))) - 1
	return b[max(i, 0)]
}
