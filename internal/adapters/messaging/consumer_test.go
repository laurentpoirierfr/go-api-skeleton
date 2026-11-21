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
		MaxDeliveries: maxDeliveries, AckWait: 5 * time.Second,
		Retries: retries, RetryInterval: 5 * time.Millisecond,
		Backoff: []time.Duration{10 * time.Millisecond},
	}
}

// startConsumer vide la DLQ et lance le consommateur jusqu'à la fin du test.
func startConsumer(t *testing.T, e env, h messaging.EventHandler, cfg messaging.ConsumerConfig) {
	t.Helper()
	dlq, err := e.js.Stream(context.Background(), messaging.DLQStreamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := dlq.Purge(context.Background()); err != nil {
		t.Fatal(err)
	}
	c, err := messaging.Consumer(e.conn, h, quiet, logger, cfg)
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
	for range 500 {
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

func dlqMessages(t *testing.T, e env) []jetstream.Msg {
	t.Helper()
	ctx := context.Background()
	s, _ := e.js.Stream(ctx, messaging.DLQStreamName)
	info, err := s.Info(ctx)
	if err != nil || info.State.Msgs == 0 {
		return nil
	}
	c, err := e.js.OrderedConsumer(ctx, messaging.DLQStreamName, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := c.Fetch(int(info.State.Msgs), jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var msgs []jetstream.Msg
	for m := range batch.Messages() {
		msgs = append(msgs, m)
	}
	return msgs
}

func newNotificationService(e env) *app.NotificationService {
	return app.NewNotificationService(e.uow, time.Now, func() string { return uuid.Must(uuid.NewV7()).String() })
}

// De bout en bout : API → outbox → forwarder → JetStream → worker → base.
func TestConsumerEndToEnd(t *testing.T) {
	e := setup(t)
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
	if ns[0].Kind != "order.created.v1" || ns[1].Kind != "order.cancelled.v1" || ns[0].Recipient != "ada@example.com" {
		t.Errorf("%+v", ns)
	}
}

// publishRaw publie un message comme le ferait le forwarder, avec un
// Nats-Msg-Id choisi : deux Nats-Msg-Id différents pour le même message
// simulent une relivraison que la déduplication du stream n'a pas vue.
func publishRaw(t *testing.T, e env, subject, messageID, natsMsgID string, headers map[string]string, data []byte) {
	t.Helper()
	m := nats.NewMsg(subject)
	m.Data = data
	for k, v := range headers {
		m.Header.Set(k, v)
	}
	m.Header.Set("_watermill_message_uuid", messageID)
	m.Header.Set(jetstream.MsgIDHeader, natsMsgID)
	if _, err := e.js.PublishMsg(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

// Un même message livré deux fois ne produit qu'une notification.
func TestConsumerDuplicate(t *testing.T) {
	e := setup(t)
	startConsumer(t, e, newNotificationService(e), config(t, 5, 1))
	id := order.NewID()
	topic, msg, err := messaging.Encode(order.OrderPlaced{OrderID: id, Customer: "ada@example.com", Total: 990,
		Lines: []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 990}}, PlacedAt: time.Now()}, "msg-dup", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{}
	for k, v := range msg.Metadata {
		headers[k] = v
	}
	publishRaw(t, e, topic, "msg-dup", "first", headers, msg.Payload)
	publishRaw(t, e, topic, "msg-dup", "second", headers, msg.Payload)
	waitFor(t, "a notification", func() bool { return len(notifications(t, e, id)) == 1 })
	time.Sleep(300 * time.Millisecond) // laisser à la seconde livraison le temps d'arriver
	if n := len(notifications(t, e, id)); n != 1 {
		t.Errorf("%d notifications, want 1", n)
	}
}

// Un message illisible part directement en DLQ, sans nouvel essai.
func TestConsumerPermanentFailure(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	startConsumer(t, e, handlerFunc(func(context.Context, string, order.Event) (bool, error) { calls.Add(1); return true, nil }), config(t, 5, 3))
	publishRaw(t, e, "orders.shipped", "msg-unknown", "msg-unknown", map[string]string{messaging.HeaderType: "order.shipped.v9"}, []byte(`{}`))
	waitFor(t, "a dead letter", func() bool { return len(dlqMessages(t, e)) == 1 })
	m := dlqMessages(t, e)[0]
	if m.Subject() != "dlq."+config(t, 0, 0).Name || !strings.Contains(m.Headers().Get("reason_poisoned"), "unknown event type") {
		t.Errorf("subject %s, headers %v", m.Subject(), m.Headers())
	}
	if calls.Load() != 0 {
		t.Errorf("the handler was called %d times", calls.Load())
	}
}

type handlerFunc func(context.Context, string, order.Event) (bool, error)

func (f handlerFunc) Handle(ctx context.Context, id string, e order.Event) (bool, error) {
	return f(ctx, id, e)
}

func publishEvent(t *testing.T, e env, messageID string) {
	t.Helper()
	topic, msg, err := messaging.Encode(order.OrderCancelled{OrderID: order.NewID(), Customer: "ada@example.com", CancelledAt: time.Now()}, messageID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{}
	for k, v := range msg.Metadata {
		headers[k] = v
	}
	publishRaw(t, e, topic, messageID, messageID, headers, msg.Payload)
}

// Une erreur passagère se résout par des relivraisons, sans DLQ.
func TestConsumerTransientFailure(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	startConsumer(t, e, handlerFunc(func(context.Context, string, order.Event) (bool, error) {
		if calls.Add(1) <= 2 {
			return false, errors.New("database unavailable")
		}
		return true, nil
	}), config(t, 5, 0))
	publishEvent(t, e, "msg-transient")
	waitFor(t, "three calls", func() bool { return calls.Load() == 3 })
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 3 || len(dlqMessages(t, e)) != 0 {
		t.Errorf("%d calls, %d dead letters", calls.Load(), len(dlqMessages(t, e)))
	}
}

// Une erreur persistante part en DLQ après le nombre maximal de livraisons :
// 3 livraisons de 2 essais chacune (1 + 1 essai rapide).
func TestConsumerExhausted(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	startConsumer(t, e, handlerFunc(func(context.Context, string, order.Event) (bool, error) {
		calls.Add(1)
		return false, errors.New("database unavailable")
	}), config(t, 3, 1))
	publishEvent(t, e, "msg-exhausted")
	waitFor(t, "a dead letter", func() bool { return len(dlqMessages(t, e)) == 1 })
	m := dlqMessages(t, e)[0]
	if reason := m.Headers().Get("reason_poisoned"); !strings.Contains(reason, "after 3 deliveries") || !strings.Contains(reason, "database unavailable") {
		t.Errorf("reason %q", reason)
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 6 {
		t.Errorf("%d calls, want 6", calls.Load())
	}
}
