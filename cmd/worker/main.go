// Command worker consomme les événements des commandes dans NATS JetStream,
// et notifie les clients. Il expose seulement ses sondes de santé en HTTP.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/google/uuid"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/messaging"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/config"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/health"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/httpmw"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/logger"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/runner"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

// run assemble et lance le binaire ; les tests l'appellent avec leurs propres
// arguments.
func run(ctx context.Context, args []string, out io.Writer) error {
	opts, printConfig, err := configOptions(args)
	if err != nil {
		return err
	}
	if printConfig {
		text, err := config.Explain(opts)
		fmt.Fprint(out, text)
		return err
	}
	cfg, err := config.Load(opts)
	if err != nil {
		return err
	}
	log := logger.New(out, cfg.Log.Format, cfg.Log.SlogLevel(), cfg.Service.Name, version.Version)
	log.Info("configuration", "http", cfg.HTTP.Address, "postgres", cfg.Postgres.Redacted(), "nats", cfg.Messaging.NATS.URL)

	if cfg.Postgres.AutoMigrate {
		v, err := postgres.Migrate(cfg.Postgres.URL)
		if err != nil {
			return err
		}
		log.Info("database migrated", "schema_version", v)
	}
	pool, err := postgres.Connect(ctx, cfg.Postgres.URL, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Le worker ne publie rien : son unité de travail n'a pas d'outbox.
	uow := postgres.NewUnitOfWork(pool, nil)
	notifications := app.NewNotificationService(uow, time.Now, func() string { return uuid.Must(uuid.NewV7()).String() })

	conn, err := messaging.Connect(cfg.Messaging.NATS.URL, cfg.Service.Name)
	if err != nil {
		return err
	}
	defer conn.Close()
	consumer, err := messaging.Consumer(conn, notifications, log,
		watermill.NewSlogLogger(log.With("component", "watermill")),
		messaging.ConsumerConfig{
			Name:          app.NotificationConsumer,
			MaxDeliveries: cfg.Consumer.MaxDeliveries,
			AckWait:       cfg.Consumer.AckWait,
			Retries:       cfg.Consumer.Retries,
			RetryInterval: cfg.Consumer.RetryInterval,
			Backoff:       cfg.Consumer.Backoff,
		})
	if err != nil {
		return err
	}

	h := health.New(2 * time.Second)
	h.Add("postgres", pool.Ping)
	h.Add("nats", messaging.Check(conn))
	mux := http.NewServeMux()
	h.Register(mux)
	srv := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           httpmw.Chain(mux, httpmw.RequestID, httpmw.AccessLog(log), httpmw.Recover(log)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return runner.Run(ctx, log,
		runner.HTTPServer(srv, cfg.Shutdown.Timeout, h.ShuttingDown),
		consumer,
	)
}

// configOptions lit les options de la ligne de commande : le fichier désigné
// par -config ou CONFIG_FILE (obligatoire alors), sinon configs/worker.yaml s'il
// existe ; -print-config affiche la configuration effective, sans démarrer.
func configOptions(args []string) (config.Options, bool, error) {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	file := fs.String("config", os.Getenv("CONFIG_FILE"), "YAML configuration file")
	printConfig := fs.Bool("print-config", false, "print the effective configuration and the source of each value, then exit")
	if err := fs.Parse(args); err != nil {
		return config.Options{}, false, err
	}
	opts := config.Options{
		File:         *file,
		FileRequired: *file != "",
		Defaults:     config.Config{Service: config.ServiceConfig{Name: "orders-worker"}, HTTP: config.HTTPConfig{Address: ":8081"}},
	}
	if opts.File == "" {
		opts.File = "configs/worker.yaml"
	}
	return opts, *printConfig, nil
}
