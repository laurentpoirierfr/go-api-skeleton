package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/httpapi"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/adapters/memory"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newServer(t *testing.T, s httpapi.OrderService, log *slog.Logger) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	httpapi.New(mux, s, log)
	return mux
}

func memoryService() *app.OrderService {
	return app.NewOrderService(memory.NewStore(), time.Now, order.NewID)
}

type response struct {
	code int
	hdr  http.Header
	body map[string]any
}

func call(t *testing.T, h http.Handler, method, path, body string) response {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	res := response{code: w.Code, hdr: w.Header()}
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &res.body); err != nil {
			t.Fatalf("%s %s: %v: %s", method, path, err, w.Body)
		}
	}
	return res
}

const validOrder = `{"customer":"ada@example.com","lines":[{"product":"Gopher plush","quantity":2,"unit_price_cents":1250},{"product":"Mug","quantity":1,"unit_price_cents":990}]}`

func TestPlaceAndGet(t *testing.T) {
	h := newServer(t, memoryService(), quiet)
	r := call(t, h, "POST", "/v1/orders", validOrder)
	if r.code != http.StatusCreated || r.body["total_cents"] != 3490.0 || r.body["status"] != "placed" || r.body["version"] != 1.0 {
		t.Fatalf("%d %v", r.code, r.body)
	}
	id, _ := r.body["id"].(string)
	if r.hdr.Get("Location") != "/v1/orders/"+id {
		t.Errorf("Location %q", r.hdr.Get("Location"))
	}
	lines, _ := r.body["lines"].([]any)
	if first, _ := lines[0].(map[string]any); first["total_cents"] != 2500.0 {
		t.Errorf("lines %v", lines)
	}
	if g := call(t, h, "GET", "/v1/orders/"+id, ""); g.code != 200 || g.body["id"] != id {
		t.Errorf("get: %d %v", g.code, g.body)
	}
}

// fieldErrors renvoie la position de chaque erreur d'une réponse RFC 9457.
func fieldErrors(r response) []string {
	var locs []string
	errs, _ := r.body["errors"].([]any)
	for _, e := range errs {
		if m, ok := e.(map[string]any); ok {
			locs = append(locs, m["location"].(string))
		}
	}
	return locs
}

func TestValidation(t *testing.T) {
	h := newServer(t, memoryService(), quiet)
	// Rejeté par le schéma (Huma), avant le domaine.
	r := call(t, h, "POST", "/v1/orders", `{"customer":"nope","lines":[{"product":"x","quantity":0,"unit_price_cents":1}]}`)
	locs := strings.Join(fieldErrors(r), " ")
	if r.code != http.StatusUnprocessableEntity || r.hdr.Get("Content-Type") != "application/problem+json" ||
		!strings.Contains(locs, "body.customer") || !strings.Contains(locs, "body.lines[0].quantity") {
		t.Errorf("schema: %d %s %v", r.code, r.hdr.Get("Content-Type"), r.body)
	}
	// Accepté par le schéma, refusé par le domaine : un produit fait d'espaces.
	r = call(t, h, "POST", "/v1/orders", `{"customer":"ada@example.com","lines":[{"product":"   ","quantity":1,"unit_price_cents":1}]}`)
	if r.code != http.StatusUnprocessableEntity || strings.Join(fieldErrors(r), " ") != "body.lines[0].product" {
		t.Errorf("domain: %d %v", r.code, r.body)
	}
}

func TestNotFound(t *testing.T) {
	h := newServer(t, memoryService(), quiet)
	if r := call(t, h, "GET", "/v1/orders/"+order.NewID().String(), ""); r.code != 404 || r.body["detail"] != "order not found" {
		t.Errorf("%d %v", r.code, r.body)
	}
	if r := call(t, h, "GET", "/v1/orders/not-a-uuid", ""); r.code != http.StatusUnprocessableEntity {
		t.Errorf("invalid id: %d %v", r.code, r.body)
	}
}

func TestCancel(t *testing.T) {
	h := newServer(t, memoryService(), quiet)
	id := call(t, h, "POST", "/v1/orders", validOrder).body["id"].(string)
	r := call(t, h, "POST", "/v1/orders/"+id+"/cancellation", `{"reason":"Ordered by mistake"}`)
	if r.code != 200 || r.body["status"] != "cancelled" || r.body["version"] != 2.0 {
		t.Fatalf("%d %v", r.code, r.body)
	}
	if r := call(t, h, "POST", "/v1/orders/"+id+"/cancellation", ""); r.code != http.StatusConflict || r.body["detail"] != "order already cancelled" {
		t.Errorf("second cancel (no body): %d %v", r.code, r.body)
	}
	if r := call(t, h, "POST", "/v1/orders/"+order.NewID().String()+"/cancellation", "{}"); r.code != 404 {
		t.Errorf("unknown: %d", r.code)
	}
}

func TestList(t *testing.T) {
	h := newServer(t, memoryService(), quiet)
	var ids []string
	for range 5 {
		ids = append(ids, call(t, h, "POST", "/v1/orders", validOrder).body["id"].(string))
	}
	call(t, h, "POST", "/v1/orders/"+ids[0]+"/cancellation", "{}")
	var seen []string
	path := "/v1/orders?limit=2"
	for range 10 {
		r := call(t, h, "GET", path, "")
		if r.code != 200 {
			t.Fatalf("%d %v", r.code, r.body)
		}
		for _, it := range r.body["items"].([]any) {
			seen = append(seen, it.(map[string]any)["id"].(string))
		}
		next, ok := r.body["next_cursor"].(string)
		if !ok {
			break
		}
		path = "/v1/orders?limit=2&cursor=" + next
	}
	if strings.Join(seen, ",") != strings.Join([]string{ids[4], ids[3], ids[2], ids[1], ids[0]}, ",") {
		t.Errorf("pagination: %v", seen)
	}
	if r := call(t, h, "GET", "/v1/orders?status=cancelled", ""); len(r.body["items"].([]any)) != 1 {
		t.Errorf("status filter: %v", r.body)
	}
	for _, q := range []string{"status=shipped", "limit=0", "limit=101", "cursor=%25%25"} {
		if r := call(t, h, "GET", "/v1/orders?"+q, ""); r.code != http.StatusUnprocessableEntity {
			t.Errorf("%s: %d", q, r.code)
		}
	}
}

// broken simule une base en panne ou une modification concurrente.
type broken struct {
	httpapi.OrderService
	err error
}

func (b broken) CancelOrder(context.Context, order.ID, string) (order.Order, error) {
	return order.Order{}, b.err
}

func TestConflictAndInternalError(t *testing.T) {
	path := "/v1/orders/" + order.NewID().String() + "/cancellation"
	if r := call(t, newServer(t, broken{err: app.ErrConflict}, quiet), "POST", path, "{}"); r.code != http.StatusConflict {
		t.Errorf("conflict: %d", r.code)
	}
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	r := call(t, newServer(t, broken{err: errors.New("pq: password authentication failed for user admin")}, log), "POST", path, "{}")
	if r.code != 500 || r.body["detail"] != "internal error" {
		t.Errorf("%d %v", r.code, r.body)
	}
	// Le détail reste dans le journal, jamais dans la réponse.
	if !strings.Contains(logs.String(), "password authentication failed") {
		t.Errorf("logs: %s", logs.String())
	}
}

// Le contrat versionné (api/openapi.yaml) doit être celui que génère le code.
// make openapi le régénère.
func TestOpenAPISpec(t *testing.T) {
	mux := http.NewServeMux()
	spec, err := httpapi.New(mux, nil, quiet).OpenAPI().YAML()
	if err != nil {
		t.Fatal(err)
	}
	const path = "../../../api/openapi.yaml"
	if os.Getenv("UPDATE_OPENAPI") != "" {
		if err := os.WriteFile(path, spec, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, spec) {
		t.Errorf("api/openapi.yaml is out of date: run make openapi (%v)", err)
	}
}
