package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app/apptest"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/testenv"
)

func TestMain(m *testing.M) { testenv.Main(m) }

func newUnitOfWork(t *testing.T, outbox postgres.OutboxFactory) *postgres.UnitOfWork {
	t.Helper()
	pool, err := postgres.Connect(context.Background(), testenv.PostgresURL(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewUnitOfWork(pool, outbox)
}

// La même suite de contrat que l'adaptateur en mémoire.
func TestContract(t *testing.T) {
	apptest.Run(t, func(t *testing.T) app.UnitOfWork { return newUnitOfWork(t, nil) })
}

// Relancer les migrations ne change rien.
func TestMigrateIdempotent(t *testing.T) {
	v, err := postgres.Migrate(testenv.PostgresURL(t))
	if err != nil || v != 2 {
		t.Errorf("version %d, err %v", v, err)
	}
}

// Sans outbox configurée, publier un événement est une erreur, et toute la
// transaction est annulée : la commande n'est pas enregistrée non plus.
func TestNoOutbox(t *testing.T) {
	s := app.NewOrderService(newUnitOfWork(t, nil), time.Now, order.NewID)
	_, err := s.PlaceOrder(context.Background(), app.PlaceOrderCommand{
		Customer: "ada@example.com", Lines: []order.Line{{Product: "Gopher", Quantity: 1, UnitPrice: 100}},
	})
	if err == nil || !strings.Contains(err.Error(), "no outbox") {
		t.Fatalf("err = %v", err)
	}
	if page, err := s.ListOrders(context.Background(), app.ListFilter{}); err != nil || len(page.Orders) != 0 {
		t.Errorf("order kept after rollback: %d, %v", len(page.Orders), err)
	}
}
