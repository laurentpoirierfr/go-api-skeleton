package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestMain isole les tests de l'environnement du développeur : sans cela, un
// LOG_LEVEL ou un POSTGRES_URL de son shell changerait les résultats. Viper
// traite une variable vide comme absente.
func TestMain(m *testing.M) {
	for _, f := range Fields() {
		os.Setenv(f.Env, "")
	}
	os.Exit(m.Run())
}

var defaults = Config{Service: ServiceConfig{Name: "api"}, HTTP: HTTPConfig{Address: ":8080"}}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaults(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://u:secret@localhost/db")
	c, err := Load(Options{Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	if c.Service.Name != "api" || c.HTTP.Address != ":8080" || c.Messaging.Broker != "nats" ||
		c.Messaging.NATS.URL != "nats://localhost:4222" || c.Postgres.MaxConns != 10 || c.Postgres.AutoMigrate ||
		c.Log.SlogLevel() != slog.LevelInfo || c.Log.Format != "json" || c.Shutdown.Timeout != 15*time.Second ||
		c.Consumer.MaxDeliveries != 5 || !slices.Equal(c.Consumer.Backoff, []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute}) {
		t.Errorf("%+v", c)
	}
	if strings.Contains(c.Postgres.Redacted(), "secret") {
		t.Errorf("password visible: %s", c.Postgres.Redacted())
	}
}

// Le fichier surcharge les valeurs par défaut, et l'environnement le fichier.
func TestPriority(t *testing.T) {
	path := writeFile(t, `
service:
  name: orders
http:
  address: ":9000"
postgres:
  url: postgres://localhost/from-file
messaging:
  nats:
    url: nats://from-file:4222
log:
  level: debug
  format: text
`)
	t.Setenv("HTTP_ADDRESS", ":9999")
	t.Setenv("MESSAGING_NATS_URL", "nats://from-env:4222")
	t.Setenv("CONSUMER_BACKOFF", "2s,10s")
	c, err := Load(Options{File: path, FileRequired: true, Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	if c.Service.Name != "orders" || c.Postgres.URL != "postgres://localhost/from-file" || c.Log.Format != "text" {
		t.Errorf("file not applied: %+v", c)
	}
	if c.HTTP.Address != ":9999" || c.Messaging.NATS.URL != "nats://from-env:4222" ||
		!slices.Equal(c.Consumer.Backoff, []time.Duration{2 * time.Second, 10 * time.Second}) {
		t.Errorf("environment not applied: %+v", c)
	}
}

func TestMissingFile(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://localhost/db")
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	if _, err := Load(Options{File: missing, Defaults: defaults}); err != nil {
		t.Errorf("optional file: %v", err)
	}
	if _, err := Load(Options{File: missing, FileRequired: true, Defaults: defaults}); err == nil {
		t.Error("a required missing file must be an error")
	}
}

func TestInvalidYAML(t *testing.T) {
	if _, err := Load(Options{File: writeFile(t, "http: [unclosed"), Defaults: defaults}); err == nil {
		t.Error("invalid YAML must be an error")
	}
}

// Toutes les erreurs sont signalées ensemble, sans jamais citer un mot de
// passe.
func TestValidationErrors(t *testing.T) {
	for k, v := range map[string]string{
		"POSTGRES_URL": "mysql://u:secret@localhost/db", "MESSAGING_NATS_URL": "http://nats",
		"LOG_LEVEL": "chatty", "LOG_FORMAT": "xml", "SHUTDOWN_TIMEOUT": "-1s", "POSTGRES_MAX_CONNS": "0",
		"CONSUMER_MAX_DELIVERIES": "0", "CONSUMER_BACKOFF": "-1s",
	} {
		t.Setenv(k, v)
	}
	_, err := Load(Options{Defaults: defaults})
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"postgres.url", "messaging.nats.url", "log.level", "log.format", "shutdown.timeout",
		"postgres.max_conns", "consumer.max_deliveries", "consumer.backoff"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s missing from:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("password in the error:\n%v", err)
	}
	t.Setenv("MESSAGING_BROKER", "rabbitmq")
	t.Setenv("POSTGRES_URL", "postgres://localhost/db")
	if _, err := Load(Options{Defaults: defaults}); err == nil || !strings.Contains(err.Error(), "messaging.broker") {
		t.Errorf("unknown broker: %v", err)
	}
}

func TestPostgresRequired(t *testing.T) {
	if _, err := Load(Options{Defaults: defaults}); err == nil || !strings.Contains(err.Error(), "postgres.url: required") {
		t.Errorf("err = %v", err)
	}
}

// Chaque clé et chaque section sont documentées : c'est ce qui rend la
// référence utile.
func TestEveryFieldDocumented(t *testing.T) {
	walk(reflect.TypeOf(Config{}), "", func(prefix string, f reflect.StructField) {
		if f.Tag.Get("mapstructure") == "" || f.Tag.Get("doc") == "" {
			t.Errorf("%s%s: mapstructure or doc tag missing", prefix, f.Name)
		}
	}, func(key, doc string) {
		if doc == "" {
			t.Errorf("section %s: doc tag missing", key)
		}
	})
}

const referencePath = "../../../configs/reference.yaml"

// generated vérifie qu'un fichier généré est à jour ; make config-reference
// les régénère tous.
func generated(t *testing.T, path, content string) {
	t.Helper()
	if os.Getenv("UPDATE_CONFIG_REFERENCE") != "" {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != content {
		t.Errorf("%s is out of date: run make config-reference (%v)", filepath.Base(path), err)
	}
}

func TestReferenceUpToDate(t *testing.T) { generated(t, referencePath, Reference()) }

var envExamples = []struct {
	binary   string
	defaults Config
	exclude  []string
}{
	{"api", APIDefaults, []string{"consumer"}},
	{"worker", WorkerDefaults, nil},
}

// Les fichiers .env d'exemple sont à jour, et suffisent seuls : chargés comme
// variables d'environnement, sans fichier YAML, ils donnent une configuration
// valide, identique à celle du fichier YAML.
func TestEnvExamples(t *testing.T) {
	for _, ex := range envExamples {
		t.Run(ex.binary, func(t *testing.T) {
			yaml := "../../../configs/" + ex.binary + ".yaml"
			content, err := EnvExample(Options{File: yaml, FileRequired: true, Defaults: ex.defaults}, "Variables d'environnement du binaire "+ex.binary, ex.exclude...)
			if err != nil {
				t.Fatal(err)
			}
			generated(t, "../../../configs/"+ex.binary+".env.example", content)

			fromYAML, err := Load(Options{File: yaml, FileRequired: true, Defaults: ex.defaults})
			if err != nil {
				t.Fatal(err)
			}
			for line := range strings.Lines(content) {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				k, v, _ := strings.Cut(line, "=")
				t.Setenv(k, v)
			}
			fromEnv, err := Load(Options{Defaults: ex.defaults})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(fromEnv, fromYAML) {
				t.Errorf("from env:\n%+v\nfrom YAML:\n%+v", fromEnv, fromYAML)
			}
		})
	}
}

// La référence, lue comme un fichier de configuration, redonne exactement les
// valeurs par défaut : elle ne ment pas.
func TestReferenceIsValid(t *testing.T) {
	_, fromDefaults, err := load(Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, fromReference, err := load(Options{File: referencePath, FileRequired: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromDefaults, fromReference) {
		t.Errorf("reference:\n%+v\ndefaults:\n%+v", fromReference, fromDefaults)
	}
}

func TestExplain(t *testing.T) {
	path := writeFile(t, "log:\n  format: text\npostgres:\n  url: postgres://u:secret@db/orders\n")
	t.Setenv("HTTP_ADDRESS", ":9999")
	out, err := Explain(Options{File: path, FileRequired: true, Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`address: ":9999"  # env HTTP_ADDRESS`,
		`format: "text"  # fichier`,
		`name: "api"  # défaut du binaire`,
		`max_conns: 10  # défaut`,
		`url: "postgres://u:xxxxx@db/orders"  # fichier`,
		`backoff: [1s, 5s, 30s, 2m]  # défaut`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("%q missing from:\n%s", want, out)
		}
	}
	if strings.Contains(out, "secret") {
		t.Errorf("secret visible:\n%s", out)
	}
	// Invalide : le texte est renvoyé quand même, avec l'erreur.
	t.Setenv("LOG_LEVEL", "chatty")
	if out, err := Explain(Options{File: path, Defaults: defaults}); err == nil || !strings.Contains(out, `level: "chatty"  # env LOG_LEVEL`) {
		t.Errorf("invalid: %v\n%s", err, out)
	}
}

// Les fichiers livrés dans configs/ sont valides.
func TestShippedFiles(t *testing.T) {
	for _, name := range []string{"api.yaml", "worker.yaml"} {
		if _, err := Load(Options{File: filepath.Join("..", "..", "..", "configs", name), FileRequired: true, Defaults: defaults}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRedpanda(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://localhost/db")
	t.Setenv("MESSAGING_BROKER", "redpanda")
	t.Setenv("MESSAGING_REDPANDA_BROKERS", "rp-1:9092,rp-2:9092")
	c, err := Load(Options{Defaults: defaults})
	if err != nil || c.Messaging.Broker != "redpanda" || !slices.Equal(c.Messaging.Redpanda.Brokers, []string{"rp-1:9092", "rp-2:9092"}) ||
		c.Messaging.Redpanda.Partitions != 3 || c.Messaging.Redpanda.ReplicationFactor != 1 {
		t.Errorf("%+v %v", c.Messaging, err)
	}
	t.Setenv("MESSAGING_REDPANDA_BROKERS", "no-port")
	t.Setenv("MESSAGING_REDPANDA_PARTITIONS", "0")
	if _, err := Load(Options{Defaults: defaults}); err == nil || !strings.Contains(err.Error(), "expected host:port") || !strings.Contains(err.Error(), "partitions 0") {
		t.Errorf("validation: %v", err)
	}
}
