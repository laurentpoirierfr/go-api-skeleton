package order

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 21, 14, 0, 0, 0, time.FixedZone("CET", 3600))

func validLines() []Line {
	return []Line{{Product: "Gopher plush", Quantity: 2, UnitPrice: 1250}, {Product: " Mug ", Quantity: 1, UnitPrice: 990}}
}

func TestPlace(t *testing.T) {
	id := NewID()
	o, ev, err := Place(id, " ada@example.com ", validLines(), now)
	if err != nil {
		t.Fatal(err)
	}
	if o.ID() != id || o.Customer() != "ada@example.com" || o.Total() != 3490 || o.Status() != StatusPlaced || o.Version() != 1 {
		t.Errorf("%+v", o.Snapshot())
	}
	if o.Lines()[1].Product != "Mug" {
		t.Errorf("product not trimmed: %q", o.Lines()[1].Product)
	}
	if !o.CreatedAt().Equal(now) || o.CreatedAt().Location() != time.UTC {
		t.Errorf("created at %v", o.CreatedAt())
	}
	if ev.EventType() != "order.created.v1" || ev.AggregateID() != id || ev.Total != 3490 || len(ev.Lines) != 2 {
		t.Errorf("%+v", ev)
	}
}

// Toutes les erreurs sont rapportées ensemble, avec le chemin de chaque champ.
func TestPlaceValidation(t *testing.T) {
	_, _, err := Place(NewID(), "not-an-email", []Line{
		{Product: "", Quantity: 0, UnitPrice: 0},
		{Product: strings.Repeat("x", 101), Quantity: 1001, UnitPrice: MaxUnitPrice + 1},
	}, now)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatal("expected a ValidationError")
	}
	var fields []string
	for _, f := range verr.Fields {
		fields = append(fields, f.Field)
	}
	want := []string{"customer", "lines[0].product", "lines[0].quantity", "lines[0].unit_price",
		"lines[1].product", "lines[1].quantity", "lines[1].unit_price"}
	if !slices.Equal(fields, want) {
		t.Errorf("fields %v, want %v", fields, want)
	}
	if _, _, err := Place(NewID(), "a@b.fr", nil, now); err == nil || !strings.Contains(err.Error(), "at least one line") {
		t.Errorf("no lines: %v", err)
	}
	// Une adresse « Nom <adresse> » n'est pas acceptée : on veut l'adresse seule.
	if _, _, err := Place(NewID(), "Ada <ada@example.com>", validLines(), now); err == nil {
		t.Error("display name accepted")
	}
}

func TestCancel(t *testing.T) {
	o, _, _ := Place(NewID(), "ada@example.com", validLines(), now)
	later := now.Add(time.Hour)
	ev, err := o.Cancel("  changed my mind ", later)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status() != StatusCancelled || o.Version() != 2 || !o.UpdatedAt().Equal(later) {
		t.Errorf("%+v", o.Snapshot())
	}
	if ev.EventType() != "order.cancelled.v1" || ev.Reason != "changed my mind" || ev.OrderID != o.ID() {
		t.Errorf("%+v", ev)
	}
	if _, err := o.Cancel("again", later); !errors.Is(err, ErrAlreadyCancelled) || o.Version() != 2 {
		t.Errorf("second cancel: %v, version %d", err, o.Version())
	}
}

func TestCancelReasonTooLong(t *testing.T) {
	o, _, _ := Place(NewID(), "ada@example.com", validLines(), now)
	if _, err := o.Cancel(strings.Repeat("x", MaxReasonLength+1), now); !errors.Is(err, ErrInvalid) || o.Status() != StatusPlaced {
		t.Errorf("err %v, status %s", err, o.Status())
	}
}

// La commande est protégée : modifier ce que renvoient ses accesseurs ne la
// change pas.
func TestEncapsulation(t *testing.T) {
	o, _, _ := Place(NewID(), "ada@example.com", validLines(), now)
	o.Lines()[0].Quantity = 99
	s := o.Snapshot()
	s.Lines[0].Quantity = 99
	if o.Lines()[0].Quantity != 2 {
		t.Error("lines modified from outside")
	}
	if r := Restore(o.Snapshot()); r.Snapshot().Total != o.Total() || r.Version() != 1 {
		t.Errorf("restore: %+v", r.Snapshot())
	}
}

func TestIDAndMoney(t *testing.T) {
	id := NewID()
	parsed, err := ParseID(id.String())
	if err != nil || parsed != id {
		t.Errorf("%v %v", parsed, err)
	}
	if _, err := ParseID("nope"); err == nil {
		t.Error("invalid ID accepted")
	}
	a, b := NewID(), NewID()
	if a.String() >= b.String() {
		t.Error("UUID v7 must sort by creation time")
	}
	if Money(3490).String() != "34.90" || Money(5).String() != "0.05" {
		t.Errorf("%s %s", Money(3490), Money(5))
	}
}
