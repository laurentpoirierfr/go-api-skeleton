// Package postgres implémente les ports de l'application avec PostgreSQL :
// pgx pour la connexion, sqlc pour les requêtes (db/, généré), golang-migrate
// pour le schéma (migrations/, embarqué dans le binaire).
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pilote pgx v5, schéma « pgx5:// »
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect ouvre le pool de connexions. La connexion est paresseuse : le
// programme démarre même si la base n'est pas encore prête, et la sonde
// /health/ready le signale.
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL URL") // l'erreur d'origine citerait le mot de passe
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Les migrations sont embarquées : le binaire migre sa propre base, sans
// fichier à déployer à côté.
//
//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applique les migrations manquantes, et renvoie la version du schéma.
func Migrate(url string) (uint, error) {
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return 0, err
	}
	// Le pilote pgx v5 de golang-migrate s'enregistre sous le schéma « pgx5 ».
	dsn := "pgx5://" + strings.TrimPrefix(strings.TrimPrefix(url, "postgres://"), "postgresql://")
	m, err := migrate.NewWithSourceInstance("iofs", source, dsn)
	if err != nil {
		return 0, fmt.Errorf("migrations: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, fmt.Errorf("migrations: %w", err)
	}
	version, dirty, err := m.Version()
	if err != nil {
		return 0, err
	}
	if dirty {
		// Une migration a échoué à mi-chemin : il faut intervenir à la main
		// (corriger, puis « migrate force »).
		return version, fmt.Errorf("schema left dirty at version %d", version)
	}
	return version, nil
}
