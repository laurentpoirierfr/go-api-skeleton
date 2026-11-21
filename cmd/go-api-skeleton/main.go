// Command go-api-skeleton : Squelette d'API Go : HTTP, PostgreSQL, outbox et NATS JetStream.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/web"
)

// version est fixée à la compilation : go build -ldflags "-X main.version=1.0.0".
var version = "dev"

func main() {
	adresse := flag.String("adresse", envOu("ADRESSE", "localhost:8080"), "adresse d'écoute")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "go-api-skeleton", "version", version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := sert(ctx, *adresse, log); err != nil {
		log.Error("arrêt sur erreur", "err", err)
		os.Exit(1)
	}
}

// sert démarre le serveur, et l'arrête proprement quand ctx est annulé :
// les requêtes en cours ont 10 secondes pour se terminer.
func sert(ctx context.Context, adresse string, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              adresse,
		Handler:           web.Routes(log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	erreurs := make(chan error, 1)
	go func() {
		log.Info("écoute", "adresse", adresse)
		erreurs <- srv.ListenAndServe()
	}()
	select {
	case err := <-erreurs:
		return err
	case <-ctx.Done():
	}
	arret, annule := context.WithTimeout(context.Background(), 10*time.Second)
	defer annule()
	log.Info("arrêt")
	if err := srv.Shutdown(arret); err != nil {
		return err
	}
	if err := <-erreurs; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOu(cle, defaut string) string {
	if v, ok := os.LookupEnv(cle); ok {
		return v
	}
	return defaut
}
