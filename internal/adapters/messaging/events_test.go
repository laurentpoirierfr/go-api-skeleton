package messaging

import (
	"slices"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

var t0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func TestEncodeDecode(t *testing.T) {
	id := order.NewID()
	placed := order.OrderPlaced{OrderID: id, Customer: "ada@example.com", Total: 2500,
		Lines: []order.Line{{Product: "Gopher", Quantity: 2, UnitPrice: 1250}}, PlacedAt: t0}
	topic, msg, err := Encode(placed, "msg-1", t0)
	if err != nil || topic != TopicOrderCreated || msg.UUID != "msg-1" {
		t.Fatalf("%s %v %v", topic, msg, err)
	}
	for k, v := range map[string]string{
		HeaderSpecVersion: "1.0", HeaderID: "msg-1", HeaderType: "order.created.v1", HeaderSource: Source,
		HeaderSubject: id.String(), HeaderTime: "2026-11-21T12:00:00Z", HeaderContentType: "application/json",
	} {
		if got := msg.Metadata.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	back, err := Decode(msg)
	got, ok := back.(order.OrderPlaced)
	if err != nil || !ok || got.OrderID != id || got.Total != 2500 || !slices.Equal(got.Lines, placed.Lines) || !got.PlacedAt.Equal(t0) {
		t.Errorf("%+v %v", back, err)
	}

	cancelled := order.OrderCancelled{OrderID: id, Customer: "ada@example.com", Reason: "mistake", CancelledAt: t0}
	topic, msg, _ = Encode(cancelled, "msg-2", t0)
	back, err = Decode(msg)
	if topic != TopicOrderCancelled || err != nil || back.(order.OrderCancelled).Reason != "mistake" {
		t.Errorf("%s %+v %v", topic, back, err)
	}
}

func TestDecodeUnknown(t *testing.T) {
	msg := message.NewMessage("x", []byte(`{}`))
	msg.Metadata.Set(HeaderType, "order.shipped.v9")
	if _, err := Decode(msg); err == nil {
		t.Error("an unknown type must be an error")
	}
	msg.Metadata.Set(HeaderType, "order.created.v1")
	msg.Payload = []byte(`not json`)
	if _, err := Decode(msg); err == nil {
		t.Error("an invalid payload must be an error")
	}
}
