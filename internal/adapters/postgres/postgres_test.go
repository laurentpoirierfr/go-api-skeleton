package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app/apptest"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// Un seul conteneur PostgreSQL pour tous les tests du package ; chaque test
// reçoit sa propre base, migrée, pour rester indépendant des autres.
var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	adminURL  string
	startErr  error
	databases atomic.Int64
)

func startPostgres(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: starts PostgreSQL in a container")
	}
	once.Do(func() {
		ctx := context.Background()
		container, startErr = tcpostgres.Run(ctx, "docker.io/postgres:17-alpine",
			tcpostgres.WithDatabase("admin"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies())
		if startErr == nil {
			adminURL, startErr = container.ConnectionString(ctx, "sslmode=disable")
		}
	})
	if startErr != nil {
		t.Fatalf("PostgreSQL container: %v", startErr)
	}
}

// newDatabase crée une base vide et migrée, et renvoie son URL.
func newDatabase(t *testing.T) string {
	t.Helper()
	startPostgres(t)
	ctx := context.Background()
	name := fmt.Sprintf("test_%d", databases.Add(1))
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	url := strings.Replace(adminURL, "/admin?", "/"+name+"?", 1)
	if _, err := postgres.Migrate(url); err != nil {
		t.Fatal(err)
	}
	return url
}

func newUnitOfWork(t *testing.T, outbox postgres.OutboxFactory) *postgres.UnitOfWork {
	t.Helper()
	pool, err := postgres.Connect(context.Background(), newDatabase(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewUnitOfWork(pool, outbox)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
	os.Exit(code) // après le nettoyage : os.Exit ne lance pas les defer
}

// La même suite de contrat que l'adaptateur en mémoire.
func TestContract(t *testing.T) {
	apptest.Run(t, func(t *testing.T) app.UnitOfWork { return newUnitOfWork(t, nil) })
}

// Relancer les migrations ne change rien.
func TestMigrateIdempotent(t *testing.T) {
	url := newDatabase(t)
	v, err := postgres.Migrate(url)
	if err != nil || v != 1 {
		t.Errorf("version %d, err %v", v, err)
	}
}

// Sans outbox configurée, publier un événement est une erreur, et toute la
// transaction est annulée : la commande n'est pas enregistrée non plus.
func TestNoOutbox(t *testing.T) {
	uow := newUnitOfWork(t, nil)
	s := app.NewOrderService(uow, time.Now, order.NewID)
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
