package app

import (
	"context"
	"fmt"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// NotificationConsumer est le nom du consommateur, dans la boîte de
// réception : deux consommateurs différents traitent chacun le même message.
const NotificationConsumer = "notifications"

// NotificationService réagit aux événements des commandes en notifiant le
// client. Il est idempotent : un événement livré deux fois ne produit qu'une
// notification.
type NotificationService struct {
	uow   UnitOfWork
	now   func() time.Time
	newID func() string
}

// NewNotificationService assemble le service.
func NewNotificationService(uow UnitOfWork, now func() time.Time, newID func() string) *NotificationService {
	return &NotificationService{uow: uow, now: now, newID: newID}
}

// Handle traite un événement reçu. messageID identifie le message (et non
// l'événement) : c'est lui que la messagerie peut livrer deux fois.
func (s *NotificationService) Handle(ctx context.Context, messageID string, e order.Event) (bool, error) {
	var recipient string
	switch ev := e.(type) {
	case order.OrderPlaced:
		recipient = ev.Customer
	case order.OrderCancelled:
		recipient = ev.Customer
	default:
		return false, fmt.Errorf("unexpected event %s", e.EventType())
	}
	created := false
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		first, err := r.Inbox.MarkProcessed(ctx, NotificationConsumer, messageID)
		if err != nil || !first {
			return err // déjà traité : rien à faire, et le message sera acquitté
		}
		created = true
		return r.Notifications.Create(ctx, Notification{
			ID: s.newID(), OrderID: e.AggregateID(), Kind: e.EventType(), Recipient: recipient, CreatedAt: s.now().UTC(),
		})
	})
	if err != nil {
		return false, fmt.Errorf("notification for %s: %w", messageID, err)
	}
	return created, nil
}
