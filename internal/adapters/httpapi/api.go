// Package httpapi expose les cas d'usage en HTTP avec Huma : les routes, les
// DTO (structures d'entrée et de sortie) et la traduction des erreurs du
// domaine en réponses RFC 9457 (application/problem+json). Le contrat OpenAPI
// 3.1 est généré à partir de ces types.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// APIVersion est la version du contrat HTTP, distincte de celle du binaire :
// elle ne change qu'avec le contrat.
const APIVersion = "1.0.0"

// OrderService est ce dont l'API a besoin : app.OrderService le satisfait.
type OrderService interface {
	PlaceOrder(ctx context.Context, cmd app.PlaceOrderCommand) (order.Order, error)
	GetOrder(ctx context.Context, id order.ID) (order.Order, error)
	ListOrders(ctx context.Context, f app.ListFilter) (app.Page, error)
	CancelOrder(ctx context.Context, id order.ID, reason string) (order.Order, error)
}

// New enregistre l'API sur mux : les routes /v1/…, le contrat (/openapi.json,
// /openapi.yaml) et sa documentation (/docs).
func New(mux *http.ServeMux, orders OrderService, log *slog.Logger) huma.API {
	cfg := huma.DefaultConfig("Orders API", APIVersion)
	cfg.Info.Description = "Place, read, list and cancel orders. Every change publishes an event (order.created.v1, order.cancelled.v1)."
	api := humago.New(mux, cfg)
	h := handlers{orders: orders, log: log}
	h.register(api)
	return api
}
