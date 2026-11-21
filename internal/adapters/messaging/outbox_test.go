package messaging_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
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
	pool   *pgxpool.Pool
	conn   *nats.Conn
	js     jetstream.JetStream
	orders *app.OrderService
	uow    *postgres.UnitOfWork
}

// setup démarre une base vide, vide le stream, et lance le forwarder.
func setup(t *testing.T) env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := postgres.Connect(ctx, testenv.PostgresURL(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := messaging.Connect(testenv.NATSURL(t), "test")
	if err != nil {
		t.Fatal(err)
	}
	js, err := messaging.EnsureStream(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := js.Stream(ctx, messaging.StreamName)
	if err := stream.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	pub, err := messaging.NewPublisher(conn, logger)
	if err != nil {
		t.Fatal(err)
	}
	fwd, err := messaging.Forwarder(pool, pub, logger, 20*time.Millisecond)
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
		conn.Close()
		pool.Close()
	})
	uow := postgres.NewUnitOfWork(pool, func(tx pgx.Tx) app.Outbox { return messaging.NewOutbox(tx, logger) })
	return env{pool: pool, conn: conn, js: js, uow: uow, orders: app.NewOrderService(uow, time.Now, order.NewID)}
}

// receive lit n messages du stream, depuis le début.
func receive(t *testing.T, js jetstream.JetStream, n int) []jetstream.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := js.OrderedConsumer(ctx, messaging.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{messaging.StreamSubjects}})
	if err != nil {
		t.Fatal(err)
	}
	var msgs []jetstream.Msg
	for len(msgs) < n {
		batch, err := c.Fetch(n-len(msgs), jetstream.FetchMaxWait(time.Until(deadline(ctx))))
		if err != nil {
			t.Fatal(err)
		}
		for m := range batch.Messages() {
			msgs = append(msgs, m)
		}
		if ctx.Err() != nil {
			t.Fatalf("received %d messages, want %d", len(msgs), n)
		}
	}
	return msgs
}

func deadline(ctx context.Context) time.Time { d, _ := ctx.Deadline(); return d }

func outboxSize(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM watermill_outbox").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Le parcours complet : la commande et son événement dans la même
// transaction, puis le forwarder publie dans JetStream et vide l'outbox.
func TestOutboxToJetStream(t *testing.T) {
	e := setup(t)
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

	msgs := receive(t, e.js, 2)
	for i, want := range []struct{ subject, typ string }{
		{messaging.TopicOrderCreated, "order.created.v1"}, {messaging.TopicOrderCancelled, "order.cancelled.v1"},
	} {
		m := msgs[i]
		h := m.Headers()
		if m.Subject() != want.subject || h.Get(messaging.HeaderType) != want.typ || h.Get(messaging.HeaderSubject) != o.ID().String() {
			t.Errorf("message %d: subject %s, headers %v", i, m.Subject(), h)
		}
		if id := h.Get(messaging.HeaderID); id == "" || h.Get(jetstream.MsgIDHeader) != id {
			t.Errorf("message %d: Nats-Msg-Id %q, ce-id %q", i, h.Get(jetstream.MsgIDHeader), id)
		}
		wm := message.NewMessage(h.Get(messaging.HeaderID), m.Data())
		for k := range h {
			wm.Metadata.Set(k, h.Get(k))
		}
		ev, err := messaging.Decode(wm)
		if err != nil || ev.AggregateID() != o.ID() {
			t.Errorf("message %d: %v %v", i, ev, err)
		}
	}
	for range 100 {
		if outboxSize(t, e.pool) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("outbox not emptied: %d messages left", outboxSize(t, e.pool))
}

// Une transaction annulée ne publie rien : l'événement disparaît avec elle.
func TestRollbackPublishesNothing(t *testing.T) {
	e := setup(t)
	boom := errors.New("boom")
	err := e.uow.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
		o, ev, err := order.Place(order.NewID(), "ada@example.com", []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 100}}, time.Now())
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
	time.Sleep(300 * time.Millisecond) // plusieurs passages du forwarder
	stream, _ := e.js.Stream(context.Background(), messaging.StreamName)
	info, err := stream.Info(context.Background())
	if err != nil || info.State.Msgs != 0 || outboxSize(t, e.pool) != 0 {
		t.Errorf("stream %d messages, outbox %d, err %v", info.State.Msgs, outboxSize(t, e.pool), err)
	}
}

// Un message republié (forwarder arrêté entre la publication et l'effacement)
// n'est stocké qu'une fois : JetStream déduplique sur Nats-Msg-Id.
func TestDeduplication(t *testing.T) {
	e := setup(t)
	pub, err := messaging.NewPublisher(e.conn, logger)
	if err != nil {
		t.Fatal(err)
	}
	ev := order.OrderCancelled{OrderID: order.NewID(), Customer: "ada@example.com", CancelledAt: time.Now()}
	for range 3 {
		topic, msg, err := messaging.Encode(ev, "same-id", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := pub.Publish(topic, msg); err != nil {
			t.Fatal(err)
		}
	}
	stream, _ := e.js.Stream(context.Background(), messaging.StreamName)
	if info, err := stream.Info(context.Background()); err != nil || info.State.Msgs != 1 {
		t.Errorf("%d messages stored, want 1 (%v)", info.State.Msgs, err)
	}
}
