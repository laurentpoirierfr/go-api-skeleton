// Package config charge la configuration avec Viper, dans cet ordre de
// priorité croissante :
//
//  1. les valeurs par défaut du binaire (dans le code) ;
//  2. le fichier YAML (configs/api.yaml, par exemple) ;
//  3. les variables d'environnement : HTTP_ADDRESS surcharge http.address,
//     POSTGRES_URL surcharge postgres.url (les points deviennent des « _ »).
//
// La configuration est validée au démarrage : un programme mal configuré
// s'arrête tout de suite, avec la liste complète des erreurs.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config est la configuration commune aux binaires. Les étiquettes
// mapstructure donnent les clés du fichier YAML.
type Config struct {
	Service  ServiceConfig  `mapstructure:"service"`
	HTTP     HTTPConfig     `mapstructure:"http"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	NATS     NATSConfig     `mapstructure:"nats"`
	Log      LogConfig      `mapstructure:"log"`
	Shutdown ShutdownConfig `mapstructure:"shutdown"`
}

type ServiceConfig struct {
	Name string `mapstructure:"name"` // nom du service dans les journaux et les traces
}

type HTTPConfig struct {
	Address string `mapstructure:"address"` // adresse d'écoute (API, ou sondes du worker)
}

type PostgresConfig struct {
	URL string `mapstructure:"url"` // obligatoire ; en production, par variable d'environnement
}

type NATSConfig struct {
	URL string `mapstructure:"url"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`  // debug, info, warn, error
	Format string `mapstructure:"format"` // json ou text
}

type ShutdownConfig struct {
	Timeout time.Duration `mapstructure:"timeout"` // temps laissé aux traitements en cours
}

// Options dit où trouver la configuration.
type Options struct {
	// File est le chemin du fichier YAML ; vide, aucun fichier n'est lu.
	File string
	// FileRequired rend le fichier obligatoire : vrai quand l'utilisateur l'a
	// désigné (-config, CONFIG_FILE), faux pour le chemin par défaut, pour
	// qu'un conteneur puisse tourner avec ses seules variables d'environnement.
	FileRequired bool
	// Defaults sont les valeurs propres au binaire (son nom, son adresse).
	Defaults Config
}

// Load charge et valide la configuration.
func Load(opts Options) (Config, error) {
	v := viper.New()
	setDefaults(v, opts.Defaults)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv() // toute clé connue est surchargée par sa variable d'environnement

	if opts.File != "" {
		v.SetConfigFile(opts.File)
		if err := v.ReadInConfig(); err != nil {
			var missing *fs.PathError
			if !errors.As(err, &missing) || opts.FileRequired {
				return Config{}, fmt.Errorf("configuration file %s: %w", opts.File, err)
			}
		}
	}
	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// setDefaults déclare toutes les clés : AutomaticEnv ne surcharge que les clés
// que Viper connaît.
func setDefaults(v *viper.Viper, d Config) {
	v.SetDefault("service.name", d.Service.Name)
	v.SetDefault("http.address", d.HTTP.Address)
	v.SetDefault("postgres.url", "")
	v.SetDefault("nats.url", "nats://localhost:4222")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("shutdown.timeout", "15s")
}

func (c Config) validate() error {
	var errs []error
	if c.Service.Name == "" {
		errs = append(errs, errors.New("service.name: required"))
	}
	if c.HTTP.Address == "" {
		errs = append(errs, errors.New("http.address: required"))
	}
	if err := checkURL(c.Postgres.URL, "postgres", "postgresql"); err != nil {
		errs = append(errs, fmt.Errorf("postgres.url: %w", err))
	}
	if err := checkURL(c.NATS.URL, "nats", "tls"); err != nil {
		errs = append(errs, fmt.Errorf("nats.url: %w", err))
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Log.Level)); err != nil {
		errs = append(errs, fmt.Errorf("log.level: %w", err))
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		errs = append(errs, fmt.Errorf("log.format: %q, expected json or text", c.Log.Format))
	}
	if c.Shutdown.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.timeout: %v, must be positive", c.Shutdown.Timeout))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	return nil
}

// SlogLevel renvoie le niveau de journal (déjà validé par Load).
func (l LogConfig) SlogLevel() slog.Level {
	var level slog.Level
	_ = level.UnmarshalText([]byte(l.Level))
	return level
}

func checkURL(raw string, schemes ...string) error {
	if raw == "" {
		return errors.New("required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("unparsable URL") // l'erreur d'origine citerait l'URL, mot de passe compris
	}
	for _, s := range schemes {
		if u.Scheme == s {
			return nil
		}
	}
	return fmt.Errorf("scheme %q, expected %s", u.Scheme, strings.Join(schemes, " or "))
}

// Redacted renvoie l'URL sans son mot de passe, pour les journaux.
func (p PostgresConfig) Redacted() string {
	u, err := url.Parse(p.URL)
	if err != nil {
		return "(unparsable)"
	}
	return u.Redacted()
}
