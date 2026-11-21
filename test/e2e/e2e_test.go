// Package e2e teste les binaires comme ils sont déployés : compilés, lancés
// comme des processus, configurés par leurs seules variables d'environnement,
// face à un vrai PostgreSQL et un vrai broker (NATS puis Redpanda), et
// utilisés par leur API HTTP. Sauté avec go test -short.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/testenv"
)

// binaries contient api et worker, compilés une fois pour tous les tests.
var binaries string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "e2e-bin-")
	if err != nil {
		panic(err)
	}
	binaries = dir
	for _, b := range []string{"api", "worker"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, b), "github.com/laurentpoirierfr/go-api-skeleton/cmd/"+b)
		if out, err := cmd.CombinedOutput(); err != nil {
			panic(fmt.Sprintf("build %s: %v\n%s", b, err, out))
		}
	}
	testenv.Main(m, func() { os.RemoveAll(dir) })
}

// process est un binaire lancé, avec son journal.
type process struct {
	name string
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan error
}

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

// start lance un binaire depuis un répertoire vide : sans configs/*.yaml, seul
// l'environnement compte, comme en production.
func start(t *testing.T, name string, env map[string]string) *process {
	t.Helper()
	cmd := exec.Command(filepath.Join(binaries, name))
	cmd.Dir = t.TempDir()
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	p := &process{name: name, cmd: cmd, out: &syncBuffer{}, done: make(chan error, 1)}
	cmd.Stdout, cmd.Stderr = p.out, p.out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			<-p.done
		}
		if t.Failed() {
			t.Logf("journal de %s :\n%s", name, p.out.String())
		}
	})
	return p
}

// stop envoie SIGTERM, et vérifie un arrêt propre (code 0) dans le délai.
func (p *process) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Errorf("%s: arrêt en erreur : %v", p.name, err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("%s: pas arrêté en 30 s", p.name)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitReady(t *testing.T, p *process, addr string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := http.Get("http://" + addr + "/health/ready"); err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case err := <-p.done:
			t.Fatalf("%s s'est arrêté : %v", p.name, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("%s pas prêt en 60 s", p.name)
}

type response struct {
	code int
	hdr  http.Header
	body map[string]any
}

func call(t *testing.T, method, url, body string) response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	res := response{code: r.StatusCode, hdr: r.Header}
	_ = json.NewDecoder(r.Body).Decode(&res.body)
	return res
}

var brokers = []struct {
	name string
	env  func(t *testing.T) map[string]string
}{
	{"nats", func(t *testing.T) map[string]string {
		return map[string]string{"MESSAGING_BROKER": "nats", "MESSAGING_NATS_URL": testenv.NATSURL(t)}
	}},
	{"redpanda", func(t *testing.T) map[string]string {
		return map[string]string{"MESSAGING_BROKER": "redpanda", "MESSAGING_REDPANDA_BROKERS": testenv.RedpandaBroker(t)}
	}},
}

// La vie d'une commande, de la requête HTTP à la notification du worker.
func TestOrderLifecycle(t *testing.T) {
	for _, broker := range brokers {
		t.Run(broker.name, func(t *testing.T) {
			dbURL := testenv.PostgresURL(t) // base vide, déjà migrée
			apiAddr, workerAddr := freeAddress(t), freeAddress(t)
			common := map[string]string{
				"POSTGRES_URL": dbURL, "LOG_FORMAT": "json", "TELEMETRY_ENABLED": "false", "SHUTDOWN_TIMEOUT": "5s",
			}
			for k, v := range broker.env(t) {
				common[k] = v
			}
			env := func(addr string) map[string]string {
				e := map[string]string{"HTTP_ADDRESS": addr}
				for k, v := range common {
					e[k] = v
				}
				return e
			}
			api := start(t, "api", env(apiAddr))
			worker := start(t, "worker", env(workerAddr))
			waitReady(t, api, apiAddr)
			waitReady(t, worker, workerAddr)
			base := "http://" + apiAddr

			// Créer, lire, lister.
			r := call(t, "POST", base+"/v1/orders", `{"customer":"ada@example.com","lines":[{"product":"Gopher","quantity":2,"unit_price_cents":1250}]}`)
			if r.code != http.StatusCreated || r.body["total_cents"] != 2500.0 {
				t.Fatalf("création : %d %v", r.code, r.body)
			}
			id := r.body["id"].(string)
			if loc := r.hdr.Get("Location"); loc != "/v1/orders/"+id {
				t.Errorf("Location %q", loc)
			}
			if r := call(t, "GET", base+"/v1/orders/"+id, ""); r.code != http.StatusOK || r.body["status"] != "placed" {
				t.Errorf("lecture : %d %v", r.code, r.body)
			}
			if r := call(t, "GET", base+"/v1/orders?limit=5", ""); r.code != http.StatusOK || len(r.body["items"].([]any)) != 1 {
				t.Errorf("liste : %d %v", r.code, r.body)
			}
			// Les erreurs du contrat.
			if r := call(t, "POST", base+"/v1/orders", `{"customer":"nope","lines":[]}`); r.code != http.StatusUnprocessableEntity || r.hdr.Get("Content-Type") != "application/problem+json" {
				t.Errorf("validation : %d %s", r.code, r.hdr.Get("Content-Type"))
			}
			// Annuler, deux fois.
			if r := call(t, "POST", base+"/v1/orders/"+id+"/cancellation", `{"reason":"e2e"}`); r.code != http.StatusOK || r.body["version"] != 2.0 {
				t.Errorf("annulation : %d %v", r.code, r.body)
			}
			if r := call(t, "POST", base+"/v1/orders/"+id+"/cancellation", ""); r.code != http.StatusConflict {
				t.Errorf("seconde annulation : %d", r.code)
			}

			// Le worker a reçu les deux événements, et notifié le client.
			kinds := waitNotifications(t, dbURL, id, 2)
			if kinds != "order.cancelled.v1,order.created.v1" {
				t.Errorf("notifications : %s", kinds)
			}
			if !strings.Contains(worker.out.String(), `"msg":"event handled"`) {
				t.Errorf("journal du worker sans « event handled »")
			}

			api.stop(t)
			worker.stop(t)
		})
	}
}

// waitNotifications attend n notifications pour la commande, et renvoie leurs
// types, triés.
func waitNotifications(t *testing.T, dbURL, orderID string, n int) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var kinds string
	for range 300 {
		err := conn.QueryRow(ctx, `SELECT coalesce(string_agg(kind, ',' ORDER BY kind), '') FROM notifications WHERE order_id = $1`, orderID).Scan(&kinds)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(kinds, ",")+1 == n && kinds != "" {
			return kinds
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s : notifications %q après 30 s", orderID, kinds)
	return ""
}
