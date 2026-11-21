// Package order est le domaine des commandes : les règles métier, sans
// aucune dépendance technique (ni base, ni HTTP, ni messagerie). Une commande
// ne peut être créée ou modifiée que par ses méthodes, qui garantissent ses
// invariants et produisent les événements correspondants.
package order

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ID identifie une commande. C'est un UUID v7 : triable dans le temps, ce qui
// sert la pagination et l'index de la base.
type ID uuid.UUID

// NewID crée un identifiant.
func NewID() ID { return ID(uuid.Must(uuid.NewV7())) }

// ParseID lit un identifiant.
func ParseID(s string) (ID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return ID{}, fmt.Errorf("invalid order ID %q", s)
	}
	return ID(u), nil
}

func (id ID) String() string { return uuid.UUID(id).String() }

// Money est un montant en centimes : jamais de flottant pour de l'argent.
type Money int64

func (m Money) String() string { return fmt.Sprintf("%d.%02d", m/100, m%100) }

// Status est l'état d'une commande.
type Status string

const (
	StatusPlaced    Status = "placed"
	StatusCancelled Status = "cancelled"
)

// Valid indique si le statut existe.
func (s Status) Valid() bool { return s == StatusPlaced || s == StatusCancelled }

// Limites métier.
const (
	MaxLines        = 50
	MaxQuantity     = 1000
	MaxUnitPrice    = Money(1_000_000_00) // un million
	MaxProductLen   = 100
	MaxReasonLength = 500
)

// Line est une ligne de commande.
type Line struct {
	Product   string
	Quantity  int
	UnitPrice Money
}

// Total renvoie le montant de la ligne.
func (l Line) Total() Money { return l.UnitPrice * Money(l.Quantity) }

// Order est une commande. Ses champs sont privés : seules ses méthodes la
// modifient, et chacune incrémente sa version (verrou optimiste).
type Order struct {
	id        ID
	customer  string
	lines     []Line
	total     Money
	status    Status
	version   int
	createdAt time.Time
	updatedAt time.Time
}

// Erreurs métier, reconnues par errors.Is.
var (
	ErrInvalid          = errors.New("invalid order")
	ErrAlreadyCancelled = errors.New("order already cancelled")
)

// FieldError décrit un champ invalide.
type FieldError struct {
	Field   string // chemin du champ : "customer", "lines[2].quantity"
	Message string
}

// ValidationError regroupe toutes les erreurs d'une demande : l'appelant les
// corrige en une fois.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid order: " + strings.Join(parts, "; ")
}

// Is fait de toute ValidationError une ErrInvalid pour errors.Is.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }

// Place crée une commande, et l'événement qui l'annonce.
func Place(id ID, customer string, lines []Line, now time.Time) (Order, OrderPlaced, error) {
	customer = strings.TrimSpace(customer)
	var errs []FieldError
	if addr, err := mail.ParseAddress(customer); err != nil || addr.Address != customer {
		errs = append(errs, FieldError{"customer", "must be a valid email address"})
	}
	switch {
	case len(lines) == 0:
		errs = append(errs, FieldError{"lines", "at least one line is required"})
	case len(lines) > MaxLines:
		errs = append(errs, FieldError{"lines", fmt.Sprintf("at most %d lines", MaxLines)})
	}
	clean := make([]Line, len(lines))
	var total Money
	for i, l := range lines {
		l.Product = strings.TrimSpace(l.Product)
		field := fmt.Sprintf("lines[%d]", i)
		if l.Product == "" || len(l.Product) > MaxProductLen {
			errs = append(errs, FieldError{field + ".product", fmt.Sprintf("1 to %d characters", MaxProductLen)})
		}
		if l.Quantity < 1 || l.Quantity > MaxQuantity {
			errs = append(errs, FieldError{field + ".quantity", fmt.Sprintf("between 1 and %d", MaxQuantity)})
		}
		if l.UnitPrice < 1 || l.UnitPrice > MaxUnitPrice {
			errs = append(errs, FieldError{field + ".unit_price", fmt.Sprintf("between 1 and %d cents", MaxUnitPrice)})
		}
		clean[i] = l
		total += l.Total() // pas de dépassement possible dans ces limites
	}
	if len(errs) > 0 {
		return Order{}, OrderPlaced{}, &ValidationError{Fields: errs}
	}
	now = now.UTC()
	o := Order{id: id, customer: customer, lines: clean, total: total, status: StatusPlaced, version: 1, createdAt: now, updatedAt: now}
	return o, OrderPlaced{OrderID: id, Customer: customer, Lines: clone(clean), Total: total, PlacedAt: now}, nil
}

// Cancel annule la commande. Annuler deux fois est une erreur : l'appelant
// sait ainsi que sa demande n'a rien changé.
func (o *Order) Cancel(reason string, now time.Time) (OrderCancelled, error) {
	if o.status == StatusCancelled {
		return OrderCancelled{}, ErrAlreadyCancelled
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > MaxReasonLength {
		return OrderCancelled{}, &ValidationError{Fields: []FieldError{{"reason", fmt.Sprintf("at most %d characters", MaxReasonLength)}}}
	}
	now = now.UTC()
	o.status, o.updatedAt = StatusCancelled, now
	o.version++
	return OrderCancelled{OrderID: o.id, Customer: o.customer, Reason: reason, CancelledAt: now}, nil
}

func (o Order) ID() ID               { return o.id }
func (o Order) Customer() string     { return o.customer }
func (o Order) Lines() []Line        { return clone(o.lines) } // une copie : l'appelant ne peut pas modifier la commande
func (o Order) Total() Money         { return o.total }
func (o Order) Status() Status       { return o.status }
func (o Order) Version() int         { return o.version }
func (o Order) CreatedAt() time.Time { return o.createdAt }
func (o Order) UpdatedAt() time.Time { return o.updatedAt }

// Snapshot est l'état complet d'une commande, pour la persistance. C'est le
// seul moyen de reconstruire une commande hors de ses méthodes métier.
type Snapshot struct {
	ID        ID
	Customer  string
	Lines     []Line
	Total     Money
	Status    Status
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Snapshot renvoie l'état de la commande.
func (o Order) Snapshot() Snapshot {
	return Snapshot{o.id, o.customer, clone(o.lines), o.total, o.status, o.version, o.createdAt, o.updatedAt}
}

// Restore reconstruit une commande lue en base.
func Restore(s Snapshot) Order {
	return Order{s.ID, s.Customer, clone(s.Lines), s.Total, s.Status, s.Version, s.CreatedAt, s.UpdatedAt}
}

func clone(lines []Line) []Line { return append([]Line(nil), lines...) }
