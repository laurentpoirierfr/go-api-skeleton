package messaging_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/messaging"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/testenv"
)

func TestMain(m *testing.M) { testenv.Main(m) }

var logger = watermill.NopLogger{}

type env struct {
	broker   messaging.Broker
	redpanda string // adresse Kafka, pour lire les topics (vide avec NATS)
	pool     *pgxpool.Pool
	orders   *app.OrderService
	uow      *postgres.UnitOfWork
}

var brokers = []struct {
	name string
	new  func(t *testing.T) (messaging.Broker, string)
}{
	{"nats", func(t *testing.T) (messaging.Broker, string) {
		b, err := messaging.NewNATS(testenv.NATSURL(t), "test", logger)
		if err != nil {
			t.Fatal(err)
		}
		return b, ""
	}},
	{"redpanda", func(t *testing.T) (messaging.Broker, string) {
		addr := testenv.RedpandaBroker(t)
		b, err := messaging.NewRedpanda(messaging.RedpandaConfig{Brokers: []string{addr}, Partitions: 3, ReplicationFactor: 1}, logger)
		if err != nil {
			t.Fatal(err)
		}
		return b, addr
	}},
}

// forEachBroker joue le test contre NATS JetStream, puis contre Redpanda.
func forEachBroker(t *testing.T, f func(t *testing.T, e env)) {
	for _, b := range brokers {
		t.Run(b.name, func(t *testing.T) { f(t, setup(t, b.new)) })
	}
}

// setup démarre une base vide, crée la topologie, et lance le forwarder.
func setup(t *testing.T, newBroker func(t *testing.T) (messaging.Broker, string)) env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := postgres.Connect(ctx, testenv.PostgresURL(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	b, addr := newBroker(t)
	topoCtx, topoCancel := context.WithTimeout(ctx, 30*time.Second)
	defer topoCancel()
	if err := messaging.WaitForTopology(topoCtx, b, 500*time.Millisecond, func(error) {}); err != nil {
		t.Fatal(err)
	}
	fwd, err := messaging.Forwarder(pool, b.Publisher(), logger, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fwd.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("forwarder: %v", err)
		}
		_ = b.Close()
		pool.Close()
	})
	uow := postgres.NewUnitOfWork(pool, func(tx pgx.Tx) app.Outbox { return messaging.NewOutbox(tx, logger) })
	return env{broker: b, redpanda: addr, pool: pool, uow: uow, orders: app.NewOrderService(uow, time.Now, order.NewID)}
}

var probes atomic.Int64

// read lit un topic depuis le début, et renvoie les messages retenus par keep,
// dès qu'il en a want ou à l'expiration de wait.
func read(t *testing.T, e env, topic string, want int, wait time.Duration, keep func(*message.Message) bool) []*message.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var (
		msgs <-chan *message.Message
		err  error
	)
	if e.redpanda != "" {
		sc := kafka.DefaultSaramaSubscriberConfig()
		sc.Consumer.Offsets.Initial = sarama.OffsetOldest
		sub, serr := kafka.NewSubscriber(kafka.SubscriberConfig{
			Brokers: []string{e.redpanda}, Unmarshaler: kafka.DefaultMarshaler{},
			ConsumerGroup: fmt.Sprintf("probe-%d", probes.Add(1)), OverwriteSaramaConfig: sc,
		}, logger)
		if serr != nil {
			t.Fatal(serr)
		}
		defer sub.Close()
		msgs, err = sub.Subscribe(ctx, topic)
	} else {
		msgs, err = readNATS(ctx, t, e, topic)
	}
	if err != nil {
		t.Fatal(err)
	}
	var kept []*message.Message
	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				return kept
			}
			m.Ack()
			if keep(m) {
				kept = append(kept, m)
				if len(kept) == want && want > 0 {
					return kept
				}
			}
		case <-ctx.Done():
			return kept
		}
	}
}

// readNATS lit un sujet du stream des événements ou de la DLQ.
func readNATS(ctx context.Context, t *testing.T, e env, subject string) (<-chan *message.Message, error) {
	t.Helper()
	js, err := jetstream.New(e.broker.(*messaging.NATS).Conn())
	if err != nil {
		return nil, err
	}
	stream := messaging.StreamName
	if strings.HasPrefix(subject, "dlq.") {
		stream = messaging.DLQStreamName
	}
	c, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	if err != nil {
		return nil, err
	}
	out := make(chan *message.Message)
	go func() {
		defer close(out)
		for ctx.Err() == nil {
			m, err := c.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
			if err != nil {
				continue
			}
			wm, err := wmjs.DefaultUnmarshaler{}.Unmarshal(m)
			if err != nil {
				continue
			}
			wm.Metadata.Set("nats-msg-id", m.Headers().Get(jetstream.MsgIDHeader))
			select {
			case out <- wm:
			case <-ctx.Done():
			}
		}
	}()
	return out, nil
}

func forOrder(id order.ID) func(*message.Message) bool {
	return func(m *message.Message) bool { return m.Metadata.Get(messaging.HeaderSubject) == id.String() }
}

func outboxSize(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM watermill_outbox").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Le parcours complet : la commande et son événement dans la même
// transaction, puis le forwarder publie et vide l'outbox.
func TestOutboxToBroker(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		ctx := context.Background()
		o, err := e.orders.PlaceOrder(ctx, app.PlaceOrderCommand{
			Customer: "ada@example.com", Lines: []order.Line{{Product: "Gopher", Quantity: 2, UnitPrice: 1250}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.orders.CancelOrder(ctx, o.ID(), "mistake"); err != nil {
			t.Fatal(err)
		}
		for _, want := range []struct{ topic, typ string }{
			{messaging.TopicOrderCreated, "order.created.v1"}, {messaging.TopicOrderCancelled, "order.cancelled.v1"},
		} {
			msgs := read(t, e, want.topic, 1, 20*time.Second, forOrder(o.ID()))
			if len(msgs) != 1 {
				t.Fatalf("%s: %d messages", want.topic, len(msgs))
			}
			m := msgs[0]
			if m.Metadata.Get(messaging.HeaderType) != want.typ || m.UUID == "" || m.UUID != m.Metadata.Get(messaging.HeaderID) {
				t.Errorf("%s: uuid %q, metadata %v", want.topic, m.UUID, m.Metadata)
			}
			if id := m.Metadata.Get("nats-msg-id"); e.redpanda == "" && id != m.UUID {
				t.Errorf("Nats-Msg-Id %q, want %q", id, m.UUID)
			}
			if ev, err := messaging.Decode(m); err != nil || ev.AggregateID() != o.ID() {
				t.Errorf("%s: %v %v", want.topic, ev, err)
			}
		}
		for range 100 {
			if outboxSize(t, e.pool) == 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("outbox not emptied: %d messages left", outboxSize(t, e.pool))
	})
}

// Une transaction annulée ne publie rien : l'événement disparaît avec elle.
func TestRollbackPublishesNothing(t *testing.T) {
	forEachBroker(t, func(t *testing.T, e env) {
		boom := errors.New("boom")
		id := order.NewID()
		err := e.uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
			o, ev, err := order.Place(id, "ada@example.com", []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 100}}, time.Now())
			if err != nil {
				return err
			}
			if err := r.Orders.Create(ctx, o); err != nil {
				return err
			}
			if err := r.Outbox.Add(ctx, ev); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if msgs := read(t, e, messaging.TopicOrderCreated, 0, 2*time.Second, forOrder(id)); len(msgs) != 0 || outboxSize(t, e.pool) != 0 {
			t.Errorf("%d messages published, outbox %d", len(msgs), outboxSize(t, e.pool))
		}
	})
}

// NATS seulement : un message republié (forwarder arrêté entre la
// publication et l'effacement) n'est stocké qu'une fois, grâce à
// Nats-Msg-Id. Redpanda n'offre pas cette garantie : l'idempotence du
// consommateur absorbe le doublon (TestConsumerDuplicate).
func TestNATSDeduplication(t *testing.T) {
	e := setup(t, brokers[0].new)
	ev := order.OrderCancelled{OrderID: order.NewID(), Customer: "ada@example.com", CancelledAt: time.Now()}
	for range 3 {
		topic, msg, err := messaging.Encode(ev, "same-id-"+ev.OrderID.String(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := e.broker.Publisher().Publish(topic, msg); err != nil {
			t.Fatal(err)
		}
	}
	if msgs := read(t, e, messaging.TopicOrderCancelled, 0, 2*time.Second, forOrder(ev.OrderID)); len(msgs) != 1 {
		t.Errorf("%d messages stored, want 1", len(msgs))
	}
}
