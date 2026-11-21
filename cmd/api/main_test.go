package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer est un tampon sûr en concurrence, pour capturer le journal.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// Le binaire démarre, répond à ses sondes, puis s'arrête proprement.
func TestRun(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	t.Setenv("HTTP_ADDRESS", addr)
	t.Setenv("POSTGRES_URL", "postgres://u:secret@localhost/db")
	t.Setenv("POSTGRES_AUTO_MIGRATE", "false") // pas de base dans ce test : la connexion est paresseuse
	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- run(ctx, []string{"-config", "../../configs/api.yaml"}, &logs) }()

	var r *http.Response
	for range 100 {
		if r, err = http.Get("http://" + addr + "/health/live"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Errorf("status %d", r.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if strings.Contains(logs.String(), "secret") {
		t.Errorf("password in the logs:\n%s", logs.String())
	}
}

func TestInvalidConfiguration(t *testing.T) {
	t.Setenv("POSTGRES_URL", "mysql://localhost/db")
	err := run(context.Background(), []string{"-config", "../../configs/api.yaml"}, &syncBuffer{})
	if err == nil || !strings.Contains(err.Error(), "postgres.url") {
		t.Errorf("err = %v", err)
	}
}

func TestMissingConfigFile(t *testing.T) {
	err := run(context.Background(), []string{"-config", "absent.yaml"}, &syncBuffer{})
	if err == nil || !strings.Contains(err.Error(), "absent.yaml") {
		t.Errorf("err = %v", err)
	}
}
