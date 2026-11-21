// Package logger crée le journal structuré (slog) des binaires.
package logger

import (
	"io"
	"log/slog"
)

// New renvoie un journal JSON (production) ou texte (développement), qui
// ajoute à chaque message le nom du service et sa version.
func New(w io.Writer, format string, level slog.Level, service, version string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewJSONHandler(w, opts)
	if format == "text" {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h).With("service", service, "version", version)
}
