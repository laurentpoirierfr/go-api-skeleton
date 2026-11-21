// Package testenv démarre les dépendances des tests d'intégration
// (PostgreSQL, NATS JetStream) avec Testcontainers : un conteneur par paquet
// de tests, partagé par ses tests. Les tests sont sautés avec go test -short.
//
// Chaque paquet qui l'utilise appelle testenv.Main depuis son TestMain, pour
// arrêter les conteneurs à la fin.
package testenv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
)

var (
	mu         sync.Mutex
	containers []testcontainers.Container

	pgOnce   sync.Once
	pgAdmin  string
	pgErr    error
	pgSerial atomic.Int64

	natsOnce sync.Once
	natsURL  string
	natsErr  error

	redpandaOnce   sync.Once
	redpandaBroker string
	redpandaErr    error
)

// RedpandaImage est l'image de Redpanda des tests (docker.io : le registre de
// Redpanda limite les téléchargements anonymes).
const RedpandaImage = "docker.io/redpandadata/redpanda:v25.2.4"

// Main lance les tests du paquet, puis arrête les conteneurs démarrés.
func Main(m *testing.M) {
	code := m.Run()
	mu.Lock()
	for _, c := range containers {
		_ = testcontainers.TerminateContainer(c)
	}
	mu.Unlock()
	os.Exit(code)
}

func register(c testcontainers.Container) {
	mu.Lock()
	defer mu.Unlock()
	containers = append(containers, c)
}

func skipShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: starts containers")
	}
}

// PostgresURL crée une base vide et migrée, et renvoie son URL : chaque test a
// la sienne, et reste indépendant des autres.
func PostgresURL(t *testing.T) string {
	t.Helper()
	skipShort(t)
	pgOnce.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "docker.io/postgres:17-alpine",
			tcpostgres.WithDatabase("admin"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies())
		if c != nil {
			register(c)
		}
		if err != nil {
			pgErr = err
			return
		}
		pgAdmin, pgErr = c.ConnectionString(ctx, "sslmode=disable")
	})
	if pgErr != nil {
		t.Fatalf("PostgreSQL container: %v", pgErr)
	}
	ctx := context.Background()
	name := fmt.Sprintf("test_%d", pgSerial.Add(1))
	conn, err := pgx.Connect(ctx, pgAdmin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	url := strings.Replace(pgAdmin, "/admin?", "/"+name+"?", 1)
	if _, err := postgres.Migrate(url); err != nil {
		t.Fatal(err)
	}
	return url
}

// NATSURL renvoie l'URL d'un serveur NATS avec JetStream. Le serveur est
// partagé : chaque test crée ses propres streams, ou les vide.
func NATSURL(t *testing.T) string {
	t.Helper()
	skipShort(t)
	natsOnce.Do(func() {
		ctx := context.Background()
		c, err := tcnats.Run(ctx, "docker.io/nats:2.12-alpine") // JetStream activé (-js)
		if c != nil {
			register(c)
		}
		if err != nil {
			natsErr = err
			return
		}
		natsURL, natsErr = c.ConnectionString(ctx)
	})
	if natsErr != nil {
		t.Fatalf("NATS container: %v", natsErr)
	}
	return natsURL
}

// RedpandaBroker renvoie l'adresse Kafka d'un serveur Redpanda, partagé : les
// tests y créent leurs topics, et filtrent ce qu'ils lisent.
func RedpandaBroker(t *testing.T) string {
	t.Helper()
	skipShort(t)
	redpandaOnce.Do(func() {
		ctx := context.Background()
		c, err := tcredpanda.Run(ctx, RedpandaImage)
		if c != nil {
			register(c)
		}
		if err != nil {
			redpandaErr = err
			return
		}
		redpandaBroker, redpandaErr = c.KafkaSeedBroker(ctx)
	})
	if redpandaErr != nil {
		t.Fatalf("Redpanda container: %v", redpandaErr)
	}
	return redpandaBroker
}
