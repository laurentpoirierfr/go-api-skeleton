package messaging_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/messaging"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// config renvoie un réglage rapide, avec un consommateur propre au test.
func config(t *testing.T, maxDeliveries, retries int) messaging.ConsumerConfig {
	return messaging.ConsumerConfig{
		Name:          strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()),
		MaxDeliveries: maxDeliveries, AckWait: 10 * time.Second,
		Retries: retries, RetryInterval: 5 * time.Millisecond,
		Backoff: []time.Duration{10 * time.Millisecond},
	}
}

// startConsumer lance le consommateur jusqu'à la fin du test.
func startConsumer(t *testing.T, e env, h messaging.EventHandler, cfg messaging.ConsumerConfig) {
	t.Helper()
	c, err := messaging.Consumer(e.broker, h, quiet, logger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("consumer: %v", err)
		}
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for range 1500 {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func notifications(t *testing.T, e env, id order.ID) []app.Notification {
	t.Helper()
	var ns []app.Notification
	if err := e.uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
		var err error
		ns, err = r.Notifications.ListByOrder(ctx, id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ns
}

func newNotificationService(e env) *app.NotificationService {
	return app.NewNotificationService(e.uow, time.Now, func() string { return uuid.Must(uuid.NewV7()).String() })
}

type handlerFunc func(context.Context, string, order.Event) (bool, error)

func (f handlerFunc) Handle(ctx context.Context, id string, e order.Event) (bool, error) {
	return f(ctx, id, e)
}

// publish publie un événement par le broker, et renvoie l'identifiant de sa
// commande.
func publish(t *testing.T, e env, ev order.Event) {
	t.Helper()
	topic, msg, err := messaging.Encode(ev, uuid.NewString(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.broker.Publisher().Publish(topic, msg); err != nil {
		t.Fatal(err)
	}
}

func cancelled() order.OrderCancelled {
	return order.OrderCancelled{OrderID: order.NewID(), Customer: "ada@example.com", CancelledAt: time.Now()}
}

// deadLetters lit la DLQ du consommateur, pour une commande.
func deadLetters(t *testing.T, e env, cfg messaging.ConsumerConfig, id order.ID, want int) []*message.Message {
	t.Helper()
	wait := 20 * time.Second
	if want == 0 {
		wait = 2 * time.Second // vérifier une absence : un délai court suffit
	}
	return read(t, e, messaging.DLQTopic(cfg.Name), want, wait, forOrder(id))
}

// De bout en bout : API → outbox → forwarder → broker → worker → base.
func TestConsumerEndToEnd(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		startConsumer(t, e, newNotificationService(e), config(t, 5, 1))
		ctx := context.Background()
		o, err := e.orders.PlaceOrder(ctx, app.PlaceOrderCommand{
			Customer: "ada@example.com", Lines: []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 990}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.orders.CancelOrder(ctx, o.ID(), ""); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "two notifications", func() bool { return len(notifications(t, e, o.ID())) == 2 })
		ns := notifications(t, e, o.ID())
		kinds := map[string]bool{ns[0].Kind: true, ns[1].Kind: true}
		if !kinds["order.created.v1"] || !kinds["order.cancelled.v1"] || ns[0].Recipient != "ada@example.com" {
			t.Errorf("%+v", ns)
		}
	})
}

// Un même message livré deux fois ne produit qu'une notification.
func TestConsumerDuplicate(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		startConsumer(t, e, newNotificationService(e), config(t, 5, 1))
		ev := order.OrderPlaced{OrderID: order.NewID(), Customer: "ada@example.com", Total: 990,
			Lines: []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 990}}, PlacedAt: time.Now()}
		topic, msg, err := messaging.Encode(ev, uuid.NewString(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		publishTwice(t, e, topic, msg)
		waitFor(t, "a notification", func() bool { return len(notifications(t, e, ev.OrderID)) == 1 })
		time.Sleep(500 * time.Millisecond) // laisser à la seconde livraison le temps d'arriver
		if n := len(notifications(t, e, ev.OrderID)); n != 1 {
			t.Errorf("%d notifications, want 1", n)
		}
	})
}

// publishTwice publie deux fois le même message. Avec NATS, il faut deux
// Nats-Msg-Id différents pour passer la déduplication du stream : c'est le
// cas d'un doublon qu'elle n'a pas vu (au-delà de sa fenêtre).
func publishTwice(t *testing.T, e env, topic string, msg *message.Message) {
	t.Helper()
	if e.redpanda != "" {
		for range 2 {
			if err := e.broker.Publisher().Publish(topic, msg.Copy()); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	js, err := jetstream.New(e.broker.(*messaging.NATS).Conn())
	if err != nil {
		t.Fatal(err)
	}
	for _, natsID := range []string{"first", "second"} {
		m := nats.NewMsg(topic)
		m.Data = msg.Payload
		for k, v := range msg.Metadata {
			m.Header.Set(k, v)
		}
		m.Header.Set("_watermill_message_uuid", msg.UUID)
		m.Header.Set(jetstream.MsgIDHeader, natsID+"-"+msg.UUID)
		if _, err := js.PublishMsg(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
}

// Un message illisible part directement en DLQ, sans nouvel essai.
func TestConsumerPermanentFailure(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		var calls atomic.Int32
		cfg := config(t, 5, 3)
		id := order.NewID()
		startConsumer(t, e, only(id, func(context.Context, string, order.Event) (bool, error) { calls.Add(1); return true, nil }), cfg)
		msg := message.NewMessage(uuid.NewString(), []byte(`{}`))
		msg.Metadata.Set(messaging.HeaderType, "order.shipped.v9")
		msg.Metadata.Set(messaging.HeaderSubject, id.String())
		if err := e.broker.Publisher().Publish(messaging.TopicOrderCreated, msg); err != nil {
			t.Fatal(err)
		}
		dl := deadLetters(t, e, cfg, id, 1)
		if len(dl) != 1 || !strings.Contains(dl[0].Metadata.Get("reason_poisoned"), "unknown event type") {
			t.Fatalf("dead letters: %v", dl)
		}
		if calls.Load() != 0 {
			t.Errorf("the handler was called %d times", calls.Load())
		}
	})
}

// Une erreur passagère se résout par des relivraisons, sans DLQ.
func TestConsumerTransientFailure(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		var calls atomic.Int32
		cfg := config(t, 5, 0)
		ev := cancelled()
		startConsumer(t, e, only(ev.OrderID, func(context.Context, string, order.Event) (bool, error) {
			if calls.Add(1) <= 2 {
				return false, errors.New("database unavailable")
			}
			return true, nil
		}), cfg)
		publish(t, e, ev)
		waitFor(t, "three calls", func() bool { return calls.Load() == 3 })
		if dl := deadLetters(t, e, cfg, ev.OrderID, 0); calls.Load() != 3 || len(dl) != 0 {
			t.Errorf("%d calls, %d dead letters", calls.Load(), len(dl))
		}
	})
}

// Une erreur persistante part en DLQ après le nombre maximal de livraisons :
// 3 livraisons de 2 essais chacune (1 + 1 essai rapide).
func TestConsumerExhausted(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		var calls atomic.Int32
		cfg := config(t, 3, 1)
		ev := cancelled()
		startConsumer(t, e, only(ev.OrderID, func(context.Context, string, order.Event) (bool, error) {
			calls.Add(1)
			return false, errors.New("database unavailable")
		}), cfg)
		publish(t, e, ev)
		dl := deadLetters(t, e, cfg, ev.OrderID, 1)
		if len(dl) != 1 {
			t.Fatalf("%d dead letters", len(dl))
		}
		if reason := dl[0].Metadata.Get("reason_poisoned"); !strings.Contains(reason, "after 3 deliveries") || !strings.Contains(reason, "database unavailable") {
			t.Errorf("reason %q", reason)
		}
		time.Sleep(300 * time.Millisecond)
		if calls.Load() != 6 {
			t.Errorf("%d calls, want 6", calls.Load())
		}
	})
}

// only applique f aux événements d'une commande, et accepte les autres : un
// nouveau consommateur lit le stream depuis le début, et reçoit aussi les
// événements des tests précédents.
func only(id order.ID, f handlerFunc) handlerFunc {
	return func(ctx context.Context, msgID string, e order.Event) (bool, error) {
		if e.AggregateID() != id {
			return true, nil
		}
		return f(ctx, msgID, e)
	}
}
