package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/runner"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/telemetry"
)

// EventHandler traite un événement reçu, et renvoie vrai s'il a produit un
// effet (faux pour un doublon). app.NotificationService le satisfait.
type EventHandler interface {
	Handle(ctx context.Context, messageID string, e order.Event) (bool, error)
}

// ConsumerConfig règle un consommateur.
type ConsumerConfig struct {
	// Name est le nom du consommateur (durable JetStream, groupe Kafka).
	Name string
	// MaxDeliveries : au-delà, un message toujours en échec part en DLQ.
	MaxDeliveries int
	// AckWait : sans acquittement dans ce délai, le message est relivré.
	AckWait time.Duration
	// Retries et RetryInterval : les essais rapides, dans le processus, avant
	// de rendre le message au broker (pour les coupures très brèves).
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

// Consumer fait du traitement des événements un composant : il attend la
// topologie du broker, puis fait tourner le Router Watermill jusqu'à
// l'annulation du contexte.
func Consumer(b Broker, h EventHandler, log *slog.Logger, wl watermill.LoggerAdapter, cfg ConsumerConfig) (runner.Component, error) {
	router, err := message.NewRouter(message.RouterConfig{CloseTimeout: cfg.AckWait}, wl)
	if err != nil {
		return runner.Component{}, err
	}
	poison, err := middleware.PoisonQueueWithFilter(b.Publisher(), DLQTopic(cfg.Name), func(err error) bool {
		return errors.Is(err, errDeadLetter)
	})
	if err != nil {
		return runner.Component{}, err
	}
	// Le premier middleware est le plus extérieur : la DLQ reçoit ce que les
	// essais n'ont pas résolu ; Recoverer change une panique en erreur.
	mws := []message.HandlerMiddleware{traceProcessing}
	if !b.DelaysRedelivery() {
		mws = append(mws, emulateRedelivery(backoff(cfg.Backoff)))
	}
	mws = append(mws,
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
	router.AddMiddleware(mws...)

	return runner.Component{Name: "consumer " + cfg.Name, Run: func(ctx context.Context) error {
		err := WaitForTopology(ctx, b, 2*time.Second, func(err error) {
			log.WarnContext(ctx, "waiting for the broker", "broker", b.Name(), "err", err)
		}, cfg)
		if err != nil {
			return nil // annulé pendant l'attente : arrêt normal
		}
		sub, err := b.Subscriber(cfg)
		if err != nil {
			return err
		}
		for _, topic := range b.SubscribeTopics() {
			router.AddConsumerHandler(cfg.Name+"@"+topic, topic, sub, handle(h, log, cfg.MaxDeliveries))
		}
		return router.Run(ctx)
	}}, nil
}

// traceProcessing reprend le contexte de trace du message (traceparent, écrit
// par l'outbox) et ouvre un span « process » : le traitement rejoint la trace
// de la requête qui a produit l'événement.
func traceProcessing(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		ctx := telemetry.Extract(msg.Context(), msg.Metadata)
		ctx, span := tracer.Start(ctx, msg.Metadata.Get(HeaderType)+" process", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.message.id", msg.UUID),
			attribute.String("messaging.consumer.group.name", message.HandlerNameFromCtx(msg.Context())),
		))
		defer span.End()
		msg.SetContext(ctx)
		out, err := next(msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return out, err
	}
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
			return err // rendu au broker, qui le relivrera
		}
		log.InfoContext(ctx, "event handled", "type", event.EventType(), "message_id", msg.UUID,
			"order_id", event.AggregateID().String(), "duplicate", !created, "delivery", deliveries)
		return nil
	}
}

// emulateRedelivery donne à un broker sans relivraison différée (Kafka) le
// comportement de JetStream : il compte les livraisons de chaque message, et
// attend le délai de backoff avant de le rendre au broker, qui le relivre
// aussitôt. Le compte est tenu en mémoire : il repart de zéro au redémarrage.
// Pendant l'attente, la partition est bloquée : c'est le modèle de Kafka,
// qui garde l'ordre.
func emulateRedelivery(delay backoff) message.HandlerMiddleware {
	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			mu.Lock()
			counts[msg.UUID]++
			n := counts[msg.UUID]
			mu.Unlock()
			msg.Metadata.Set(DeliveryCountKey, strconv.Itoa(n))
			out, err := h(msg)
			if err == nil { // traité, ou parti en DLQ
				mu.Lock()
				delete(counts, msg.UUID)
				mu.Unlock()
				return out, nil
			}
			select {
			case <-time.After(delay.WaitTime(uint64(n))):
			case <-msg.Context().Done():
			}
			return out, err
		}
	}
}

// deliveryCountUnmarshaler ajoute aux métadonnées le numéro de livraison que
// JetStream fournit, et que le décodeur par défaut ne transmet pas.
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
