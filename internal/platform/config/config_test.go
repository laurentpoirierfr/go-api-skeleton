package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var defaults = Config{Service: ServiceConfig{Name: "api"}, HTTP: HTTPConfig{Address: ":8080"}}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsAndEnv(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://u:secret@localhost/db")
	c, err := Load(Options{Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	if c.Service.Name != "api" || c.HTTP.Address != ":8080" || c.NATS.URL != "nats://localhost:4222" ||
		c.Log.SlogLevel() != slog.LevelInfo || c.Log.Format != "json" || c.Shutdown.Timeout != 15*time.Second {
		t.Errorf("%+v", c)
	}
	if strings.Contains(c.Postgres.Redacted(), "secret") {
		t.Errorf("password visible: %s", c.Postgres.Redacted())
	}
}

// Le fichier surcharge les valeurs par défaut, et l'environnement surcharge
// le fichier.
func TestPriority(t *testing.T) {
	path := writeFile(t, `
service:
  name: orders
http:
  address: ":9000"
postgres:
  url: postgres://localhost/from-file
log:
  level: debug
  format: text
shutdown:
  timeout: 3s
`)
	t.Setenv("HTTP_ADDRESS", ":9999")
	t.Setenv("SHUTDOWN_TIMEOUT", "7s")
	t.Setenv("POSTGRES_AUTO_MIGRATE", "true")
	c, err := Load(Options{File: path, FileRequired: true, Defaults: defaults})
	if err != nil {
		t.Fatal(err)
	}
	if c.Service.Name != "orders" || c.Postgres.URL != "postgres://localhost/from-file" ||
		c.Log.SlogLevel() != slog.LevelDebug || c.Log.Format != "text" {
		t.Errorf("file not applied: %+v", c)
	}
	if c.HTTP.Address != ":9999" || c.Shutdown.Timeout != 7*time.Second || !c.Postgres.AutoMigrate || c.Postgres.MaxConns != 10 {
		t.Errorf("environment not applied: %+v", c)
	}
}

func TestMissingFile(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://localhost/db")
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	// Le fichier par défaut est facultatif…
	if _, err := Load(Options{File: missing, Defaults: defaults}); err != nil {
		t.Errorf("optional file: %v", err)
	}
	// …mais un fichier désigné par l'utilisateur doit exister.
	if _, err := Load(Options{File: missing, FileRequired: true, Defaults: defaults}); err == nil {
		t.Error("a required missing file must be an error")
	}
}

func TestInvalidYAML(t *testing.T) {
	if _, err := Load(Options{File: writeFile(t, "http: [unclosed"), Defaults: defaults}); err == nil {
		t.Error("invalid YAML must be an error")
	}
}

// Toutes les erreurs sont signalées ensemble, sans jamais citer le mot de
// passe d'une URL.
func TestValidationErrors(t *testing.T) {
	t.Setenv("POSTGRES_URL", "mysql://u:secret@localhost/db")
	t.Setenv("NATS_URL", "http://nats")
	t.Setenv("LOG_LEVEL", "chatty")
	t.Setenv("LOG_FORMAT", "xml")
	t.Setenv("SHUTDOWN_TIMEOUT", "-1s")
	_, err := Load(Options{Defaults: defaults})
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"postgres.url", "nats.url", "log.level", "log.format", "shutdown.timeout"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s missing from:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("password in the error:\n%v", err)
	}
}

func TestPostgresRequired(t *testing.T) {
	if _, err := Load(Options{Defaults: defaults}); err == nil || !strings.Contains(err.Error(), "postgres.url: required") {
		t.Errorf("err = %v", err)
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

// TestMain isole les tests de l'environnement du développeur : sans cela, un
// LOG_LEVEL ou un POSTGRES_URL de son shell changerait les résultats. Viper
// traite une variable vide comme absente.
func TestMain(m *testing.M) {
	for _, key := range []string{"SERVICE_NAME", "HTTP_ADDRESS", "POSTGRES_URL", "NATS_URL", "LOG_LEVEL", "LOG_FORMAT", "SHUTDOWN_TIMEOUT", "POSTGRES_MAX_CONNS", "POSTGRES_AUTO_MIGRATE"} {
		os.Setenv(key, "")
	}
	os.Exit(m.Run())
}
