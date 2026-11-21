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

	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/config"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/platform/health"
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

// run assemble et lance le binaire ; les tests l'appellent avec leurs propres
// arguments.
func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	log := logger.New(out, cfg.Log.Format, cfg.Log.SlogLevel(), cfg.Service.Name, version.Version)
	log.Info("configuration", "http", cfg.HTTP.Address, "postgres", cfg.Postgres.Redacted(), "nats", cfg.NATS.URL)

	h := health.New(2 * time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	srv := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	return runner.Run(ctx, log, runner.HTTPServer(srv, cfg.Shutdown.Timeout, h.ShuttingDown))
}

// loadConfig lit le fichier désigné par -config ou CONFIG_FILE (obligatoire
// alors), sinon configs/api.yaml s'il existe, puis l'environnement.
func loadConfig(args []string) (config.Config, error) {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	file := fs.String("config", os.Getenv("CONFIG_FILE"), "YAML configuration file")
	if err := fs.Parse(args); err != nil {
		return config.Config{}, err
	}
	opts := config.Options{
		File:         *file,
		FileRequired: *file != "",
		Defaults:     config.Config{Service: config.ServiceConfig{Name: "orders-api"}, HTTP: config.HTTPConfig{Address: ":8080"}},
	}
	if opts.File == "" {
		opts.File = "configs/api.yaml"
	}
	return config.Load(opts)
}
