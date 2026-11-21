package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	log := New(&b, "json", slog.LevelInfo, "api", "1.0.0")
	log.Debug("ignored")
	log.Info("started", "port", 8080)
	var m map[string]any
	if err := json.Unmarshal(b.Bytes(), &m); err != nil {
		t.Fatalf("%v: %s", err, b.String())
	}
	if m["service"] != "api" || m["version"] != "1.0.0" || m["msg"] != "started" {
		t.Errorf("%v", m)
	}
}

func TestText(t *testing.T) {
	var b bytes.Buffer
	New(&b, "text", slog.LevelDebug, "worker", "dev").Debug("visible")
	if !strings.Contains(b.String(), "msg=visible service=worker version=dev") {
		t.Errorf("%s", b.String())
	}
}
