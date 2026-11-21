// Command api expose l'API HTTP des commandes (orders). Il porte aussi le relais
// de l'outbox, qui publie les événements dans NATS JetStream.
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
	"github.com/jackc/pgx/v5"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/httpapi"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/messaging"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/postgres"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
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
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

// outboxPollInterval est le délai entre deux lectures de l'outbox : la latence
// maximale entre l'enregistrement d'un événement et sa publication.
const outboxPollInterval = 200 * time.Millisecond

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

	// La messagerie : les événements sont écrits dans l'outbox, avec les
	// données, puis le forwarder les publie dans NATS JetStream.
	wmLogger := watermill.NewSlogLogger(log.With("component", "watermill"))
	conn, err := messaging.Connect(cfg.Messaging.NATS.URL, cfg.Service.Name)
	if err != nil {
		return err
	}
	defer conn.Close()
	publisher, err := messaging.NewPublisher(conn, wmLogger)
	if err != nil {
		return err
	}
	forwarder, err := messaging.Forwarder(pool, publisher, wmLogger, outboxPollInterval)
	if err != nil {
		return err
	}

	uow := postgres.NewUnitOfWork(pool, func(tx pgx.Tx) app.Outbox { return messaging.NewOutbox(tx, wmLogger) })
	orders := app.NewOrderService(uow, time.Now, order.NewID)

	h := health.New(2 * time.Second)
	h.Add("postgres", pool.Ping)
	h.Add("nats", messaging.Check(conn))
	mux := http.NewServeMux()
	h.Register(mux)
	httpapi.New(mux, orders, log)

	srv := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           httpmw.Chain(mux, httpmw.RequestID, httpmw.AccessLog(log), httpmw.Recover(log)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	return runner.Run(ctx, log,
		runner.HTTPServer(srv, cfg.Shutdown.Timeout, h.ShuttingDown),
		// L'API accepte des commandes même si NATS est indisponible : les
		// événements attendent dans l'outbox, et le forwarder attend le stream.
		runner.Component{Name: forwarder.Name, Run: func(ctx context.Context) error {
			_, err := messaging.WaitForStream(ctx, conn, 2*time.Second, func(err error) {
				log.WarnContext(ctx, "waiting for the NATS stream", "err", err)
			})
			if err != nil {
				return nil // annulé pendant l'attente : arrêt normal
			}
			return forwarder.Run(ctx)
		}},
	)
}

// configOptions lit les options de la ligne de commande : le fichier désigné
// par -config ou CONFIG_FILE (obligatoire alors), sinon configs/api.yaml s'il
// existe ; -print-config affiche la configuration effective, sans démarrer.
func configOptions(args []string) (config.Options, bool, error) {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	file := fs.String("config", os.Getenv("CONFIG_FILE"), "YAML configuration file")
	printConfig := fs.Bool("print-config", false, "print the effective configuration and the source of each value, then exit")
	if err := fs.Parse(args); err != nil {
		return config.Options{}, false, err
	}
	opts := config.Options{
		File:         *file,
		FileRequired: *file != "",
		Defaults:     config.Config{Service: config.ServiceConfig{Name: "orders-api"}, HTTP: config.HTTPConfig{Address: ":8080"}},
	}
	if opts.File == "" {
		opts.File = "configs/api.yaml"
	}
	return opts, *printConfig, nil
}
