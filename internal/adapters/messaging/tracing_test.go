package messaging_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/telemetry"
)

// Le traitement par le worker rejoint la trace de la requête qui a créé la
// commande : le contexte traverse l'outbox, le forwarder et le broker.
func TestTracePropagation(t *testing.T) {
	if _, err := telemetry.Setup(context.Background(), telemetry.Options{}); err != nil { // propagateur W3C
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	before := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(before) })

	forEachBroker(t, func(t *testing.T, e env) {
		startConsumer(t, e, newNotificationService(e), config(t, 5, 1))
		ctx, root := otel.Tracer("test").Start(context.Background(), "POST /v1/orders")
		o, err := e.orders.PlaceOrder(ctx, app.PlaceOrderCommand{
			Customer: "ada@example.com", Lines: []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 990}},
		})
		root.End()
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, "a notification", func() bool { return len(notifications(t, e, o.ID())) == 1 })
		time.Sleep(100 * time.Millisecond) // le span « process » se termine après la transaction

		traceID := root.SpanContext().TraceID()
		var send, process sdktrace.ReadOnlySpan
		for _, s := range rec.Ended() {
			if s.SpanContext().TraceID() != traceID {
				continue
			}
			switch {
			case s.Name() == "orders.created send" && s.SpanKind() == trace.SpanKindProducer:
				send = s
			case s.Name() == "order.created.v1 process" && s.SpanKind() == trace.SpanKindConsumer:
				process = s
			}
		}
		if send == nil || process == nil {
			t.Fatalf("send %v, process %v in trace %s", send != nil, process != nil, traceID)
		}
		if process.Parent().SpanID() != send.SpanContext().SpanID() {
			t.Errorf("process span parent %s, want the send span %s", process.Parent().SpanID(), send.SpanContext().SpanID())
		}
	})
}
