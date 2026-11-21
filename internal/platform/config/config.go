// Package config charge la configuration avec Viper, dans cet ordre de
// priorité croissante :
//
//  1. les valeurs par défaut, déclarées dans les étiquettes de Config ;
//  2. le fichier YAML (configs/api.yaml, par exemple) ;
//  3. les variables d'environnement : la clé en majuscules, les points
//     remplacés par « _ » (messaging.nats.url → MESSAGING_NATS_URL).
//
// Les étiquettes de Config sont la seule source de vérité : elles donnent les
// valeurs par défaut, le fichier de référence (configs/reference.yaml) et
// l'affichage de la configuration effective (-print-config).
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Étiquettes des champs :
//
//	mapstructure  la clé dans le fichier YAML
//	default       la valeur par défaut (une liste : valeurs séparées par « , »)
//	doc           la documentation, reprise dans configs/reference.yaml
//	secret        "true" : jamais affichée en clair

// Config est la configuration des deux binaires.
type Config struct {
	Service   ServiceConfig   `mapstructure:"service" doc:"Identité du service"`
	HTTP      HTTPConfig      `mapstructure:"http" doc:"Serveur HTTP (l'API, ou les sondes du worker)"`
	Postgres  PostgresConfig  `mapstructure:"postgres" doc:"Base de données PostgreSQL"`
	Messaging MessagingConfig `mapstructure:"messaging" doc:"Messagerie : publication et consommation des événements"`
	Consumer  ConsumerConfig  `mapstructure:"consumer" doc:"Consommation des événements (worker seulement)"`
	Log       LogConfig       `mapstructure:"log" doc:"Journal"`
	Shutdown  ShutdownConfig  `mapstructure:"shutdown" doc:"Arrêt"`
}

type ServiceConfig struct {
	Name string `mapstructure:"name" doc:"Nom du service dans les journaux et les traces ; par défaut orders-api ou orders-worker"`
}

type HTTPConfig struct {
	Address string `mapstructure:"address" doc:"Adresse d'écoute ; par défaut :8080 (api) ou :8081 (worker)"`
}

type PostgresConfig struct {
	URL         string `mapstructure:"url" secret:"true" doc:"URL de connexion (obligatoire) ; en production, par la variable d'environnement"`
	MaxConns    int32  `mapstructure:"max_conns" default:"10" doc:"Taille maximale du pool de connexions"`
	AutoMigrate bool   `mapstructure:"auto_migrate" default:"false" doc:"Appliquer les migrations au démarrage (pratique en développement)"`
}

type MessagingConfig struct {
	Broker string     `mapstructure:"broker" default:"nats" doc:"Broker utilisé : nats"`
	NATS   NATSConfig `mapstructure:"nats" doc:"NATS JetStream"`
}

type NATSConfig struct {
	URL string `mapstructure:"url" default:"nats://localhost:4222" secret:"true" doc:"URL du serveur (plusieurs : séparées par des virgules)"`
}

type ConsumerConfig struct {
	MaxDeliveries int             `mapstructure:"max_deliveries" default:"5" doc:"Au-delà de ce nombre de livraisons, un message en échec part en DLQ"`
	AckWait       time.Duration   `mapstructure:"ack_wait" default:"30s" doc:"Sans acquittement dans ce délai, le message est relivré"`
	Retries       int             `mapstructure:"retries" default:"3" doc:"Essais rapides dans le processus, avant de rendre le message au broker"`
	RetryInterval time.Duration   `mapstructure:"retry_interval" default:"100ms" doc:"Délai avant le premier essai rapide (doublé ensuite)"`
	Backoff       []time.Duration `mapstructure:"backoff" default:"1s,5s,30s,2m" doc:"Délai avant chaque relivraison ; le dernier s'applique ensuite"`
}

type LogConfig struct {
	Level  string `mapstructure:"level" default:"info" doc:"Niveau : debug, info, warn ou error"`
	Format string `mapstructure:"format" default:"json" doc:"Format : json (production) ou text (développement)"`
}

type ShutdownConfig struct {
	Timeout time.Duration `mapstructure:"timeout" default:"15s" doc:"Temps laissé aux traitements en cours, à l'arrêt"`
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
	_, c, err := load(opts)
	if err != nil {
		return Config{}, err
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func load(opts Options) (*viper.Viper, Config, error) {
	v := viper.New()
	for _, f := range Fields() {
		v.SetDefault(f.Key, f.defaultValue())
	}
	// Les valeurs propres au binaire l'emportent sur les étiquettes.
	if opts.Defaults.Service.Name != "" {
		v.SetDefault("service.name", opts.Defaults.Service.Name)
	}
	if opts.Defaults.HTTP.Address != "" {
		v.SetDefault("http.address", opts.Defaults.HTTP.Address)
	}
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv() // toute clé déclarée est surchargée par sa variable d'environnement

	if opts.File != "" {
		v.SetConfigFile(opts.File)
		if err := v.ReadInConfig(); err != nil {
			var missing *fs.PathError
			if !errors.As(err, &missing) || opts.FileRequired {
				return nil, Config{}, fmt.Errorf("configuration file %s: %w", opts.File, err)
			}
		}
	}
	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, Config{}, fmt.Errorf("configuration: %w", err)
	}
	return v, c, nil
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
	if c.Postgres.MaxConns < 1 || c.Postgres.MaxConns > 1000 {
		errs = append(errs, fmt.Errorf("postgres.max_conns: %d, expected 1 to 1000", c.Postgres.MaxConns))
	}
	switch c.Messaging.Broker {
	case "nats":
		for _, u := range strings.Split(c.Messaging.NATS.URL, ",") {
			if err := checkURL(strings.TrimSpace(u), "nats", "tls"); err != nil {
				errs = append(errs, fmt.Errorf("messaging.nats.url: %w", err))
			}
		}
	default:
		errs = append(errs, fmt.Errorf("messaging.broker: %q, expected nats", c.Messaging.Broker))
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
	errs = append(errs, c.Consumer.validate()...)
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	return nil
}

func (c ConsumerConfig) validate() []error {
	var errs []error
	if c.MaxDeliveries < 1 {
		errs = append(errs, fmt.Errorf("consumer.max_deliveries: %d, must be at least 1", c.MaxDeliveries))
	}
	if c.AckWait <= 0 {
		errs = append(errs, fmt.Errorf("consumer.ack_wait: %v, must be positive", c.AckWait))
	}
	if c.Retries < 0 || c.RetryInterval <= 0 {
		errs = append(errs, fmt.Errorf("consumer.retries and retry_interval: %d, %v", c.Retries, c.RetryInterval))
	}
	if len(c.Backoff) == 0 {
		errs = append(errs, errors.New("consumer.backoff: at least one delay"))
	}
	for _, d := range c.Backoff {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("consumer.backoff: %v, must be positive", d))
		}
	}
	return errs
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
func (p PostgresConfig) Redacted() string { return redact(p.URL) }

// redact masque le mot de passe d'une URL, ou toute autre valeur secrète.
func redact(value string) string {
	if value == "" {
		return ""
	}
	if u, err := url.Parse(value); err == nil && u.Scheme != "" {
		return u.Redacted()
	}
	return "xxxxx"
}

// Field décrit une clé de configuration, d'après les étiquettes de Config.
type Field struct {
	Key     string // postgres.max_conns
	Env     string // POSTGRES_MAX_CONNS
	Default string // telle qu'écrite dans l'étiquette
	Doc     string
	Secret  bool
	kind    reflect.Type
}

// Section décrit un groupe de clés (une structure de Config).
type Section struct {
	Key string
	Doc string
}

// Env donne le nom de la variable d'environnement d'une clé.
func Env(key string) string { return strings.ToUpper(strings.ReplaceAll(key, ".", "_")) }

// Fields liste toutes les clés, dans l'ordre de Config.
func Fields() []Field {
	var fields []Field
	walk(reflect.TypeOf(Config{}), "", func(prefix string, f reflect.StructField) {
		key := prefix + f.Tag.Get("mapstructure")
		fields = append(fields, Field{
			Key: key, Env: Env(key), Default: f.Tag.Get("default"), Doc: f.Tag.Get("doc"),
			Secret: f.Tag.Get("secret") == "true", kind: f.Type,
		})
	}, nil)
	return fields
}

func walk(t reflect.Type, prefix string, leaf func(prefix string, f reflect.StructField), section func(key, doc string)) {
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Type.Kind() == reflect.Struct {
			key := prefix + f.Tag.Get("mapstructure")
			if section != nil {
				section(key, f.Tag.Get("doc"))
			}
			walk(f.Type, key+".", leaf, section)
			continue
		}
		leaf(prefix, f)
	}
}

// defaultValue convertit l'étiquette default pour Viper : une liste pour un
// champ de type slice, du texte sinon (Viper convertit ensuite).
func (f Field) defaultValue() any {
	if f.kind.Kind() == reflect.Slice {
		if f.Default == "" {
			return []string{}
		}
		return strings.Split(f.Default, ",")
	}
	return f.Default
}

// yamlValue écrit une valeur pour un fichier YAML.
func (f Field) yamlValue(value string) string {
	switch {
	case f.kind.Kind() == reflect.Slice:
		if value == "" {
			return "[]"
		}
		return "[" + strings.ReplaceAll(value, ",", ", ") + "]"
	case f.kind.Kind() == reflect.String:
		return strconv.Quote(value)
	case value == "":
		return `""`
	}
	return value
}

// Reference renvoie le fichier de référence : toutes les clés, avec leur
// documentation, leur variable d'environnement et leur valeur par défaut.
func Reference() string {
	var b strings.Builder
	b.WriteString(`# Référence de la configuration, générée à partir du code
# (internal/platform/config) : ne pas modifier à la main, lancer
# « make config-reference ».
#
# Ordre de priorité : valeurs par défaut (ci-dessous), puis le fichier YAML du
# binaire (configs/api.yaml, configs/worker.yaml, ou -config), puis les
# variables d'environnement. Chaque clé a sa variable : la clé en majuscules,
# les points remplacés par « _ ».
#
# Pour voir la configuration effective d'un binaire, et d'où vient chaque
# valeur : go run ./cmd/api -print-config
`)
	fields := Fields()
	i := 0
	walk(reflect.TypeOf(Config{}), "", func(string, reflect.StructField) {
		f := fields[i]
		i++
		indent := strings.Repeat("  ", strings.Count(f.Key, "."))
		fmt.Fprintf(&b, "%s# %s\n%s# env : %s\n", indent, f.Doc, indent, f.Env)
		if f.Secret {
			fmt.Fprintf(&b, "%s# secret : jamais affiché en clair\n", indent)
		}
		fmt.Fprintf(&b, "%s%s: %s\n", indent, f.Key[strings.LastIndex(f.Key, ".")+1:], f.yamlValue(f.Default))
	}, func(key, doc string) {
		indent := strings.Repeat("  ", strings.Count(key, "."))
		if indent == "" {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "%s# %s\n%s%s:\n", indent, doc, indent, key[strings.LastIndex(key, ".")+1:])
	})
	return b.String()
}

// Explain renvoie la configuration effective, chaque valeur suivie de sa
// source (défaut, binaire, fichier ou variable d'environnement), les secrets
// masqués. L'erreur signale une configuration invalide : le texte est
// renvoyé quand même, pour comprendre pourquoi.
func Explain(opts Options) (string, error) {
	v, c, err := load(opts)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if opts.File != "" && v.ConfigFileUsed() != "" {
		if _, statErr := os.Stat(opts.File); statErr == nil {
			fmt.Fprintf(&b, "# fichier : %s\n", opts.File)
		}
	}
	fields := Fields()
	i := 0
	walk(reflect.TypeOf(Config{}), "", func(string, reflect.StructField) {
		f := fields[i]
		i++
		value := v.GetString(f.Key)
		if f.kind.Kind() == reflect.Slice {
			value = strings.Join(v.GetStringSlice(f.Key), ",")
		}
		if f.Secret {
			value = redact(value)
		}
		source := "défaut"
		switch {
		case os.Getenv(f.Env) != "":
			source = "env " + f.Env
		case v.InConfig(f.Key):
			source = "fichier"
		case (f.Key == "service.name" && opts.Defaults.Service.Name != "") ||
			(f.Key == "http.address" && opts.Defaults.HTTP.Address != ""):
			source = "défaut du binaire"
		}
		indent := strings.Repeat("  ", strings.Count(f.Key, "."))
		fmt.Fprintf(&b, "%s%s: %s  # %s\n", indent, f.Key[strings.LastIndex(f.Key, ".")+1:], f.yamlValue(value), source)
	}, func(key, _ string) {
		indent := strings.Repeat("  ", strings.Count(key, "."))
		fmt.Fprintf(&b, "%s%s:\n", indent, key[strings.LastIndex(key, ".")+1:])
	})
	return b.String(), c.validate()
}
