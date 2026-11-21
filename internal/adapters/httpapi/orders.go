package httpapi

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

type handlers struct {
	orders OrderService
	log    *slog.Logger
}

func (h handlers) register(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "place-order", Method: http.MethodPost, Path: "/v1/orders", DefaultStatus: http.StatusCreated,
		Summary: "Place an order", Tags: []string{"orders"},
		Description: "Creates the order and publishes order.created.v1.",
	}, h.placeOrder)
	huma.Register(api, huma.Operation{
		OperationID: "get-order", Method: http.MethodGet, Path: "/v1/orders/{id}",
		Summary: "Get an order", Tags: []string{"orders"},
	}, h.getOrder)
	huma.Register(api, huma.Operation{
		OperationID: "list-orders", Method: http.MethodGet, Path: "/v1/orders",
		Summary: "List orders", Tags: []string{"orders"},
		Description: "Most recent first, paginated with an opaque cursor.",
	}, h.listOrders)
	huma.Register(api, huma.Operation{
		OperationID: "cancel-order", Method: http.MethodPost, Path: "/v1/orders/{id}/cancellation",
		Summary: "Cancel an order", Tags: []string{"orders"},
		Description: "Cancels the order and publishes order.cancelled.v1. Cancelling twice returns 409.",
	}, h.cancelOrder)
}

func (h handlers) placeOrder(ctx context.Context, in *PlaceOrderInput) (*CreatedOrderOutput, error) {
	lines := make([]order.Line, len(in.Body.Lines))
	for i, l := range in.Body.Lines {
		lines[i] = order.Line{Product: l.Product, Quantity: l.Quantity, UnitPrice: order.Money(l.UnitPriceCents)}
	}
	o, err := h.orders.PlaceOrder(ctx, app.PlaceOrderCommand{Customer: in.Body.Customer, Lines: lines})
	if err != nil {
		return nil, h.problem(ctx, err, "body")
	}
	return &CreatedOrderOutput{Location: "/v1/orders/" + o.ID().String(), Body: toBody(o)}, nil
}

func (h handlers) getOrder(ctx context.Context, in *OrderIDInput) (*OrderOutput, error) {
	id, err := order.ParseID(in.ID)
	if err != nil {
		return nil, huma.Error404NotFound("order not found")
	}
	o, err := h.orders.GetOrder(ctx, id)
	if err != nil {
		return nil, h.problem(ctx, err, "")
	}
	return &OrderOutput{Body: toBody(o)}, nil
}

func (h handlers) listOrders(ctx context.Context, in *ListOrdersInput) (*OrderPageOutput, error) {
	f := app.ListFilter{Status: order.Status(in.Status), Limit: in.Limit}
	if in.Cursor != "" {
		before, ok := decodeCursor(in.Cursor)
		if !ok {
			return nil, huma.Error422UnprocessableEntity("invalid cursor",
				&huma.ErrorDetail{Location: "query.cursor", Message: "unknown cursor", Value: in.Cursor})
		}
		f.Before = before
	}
	page, err := h.orders.ListOrders(ctx, f)
	if err != nil {
		return nil, h.problem(ctx, err, "query")
	}
	out := &OrderPageOutput{}
	out.Body.Items = make([]OrderBody, len(page.Orders))
	for i, o := range page.Orders {
		out.Body.Items[i] = toBody(o)
	}
	if page.Next != (order.ID{}) {
		out.Body.NextCursor = encodeCursor(page.Next)
	}
	return out, nil
}

func (h handlers) cancelOrder(ctx context.Context, in *CancelOrderInput) (*OrderOutput, error) {
	id, err := order.ParseID(in.ID)
	if err != nil {
		return nil, huma.Error404NotFound("order not found")
	}
	var reason string
	if in.Body != nil {
		reason = in.Body.Reason
	}
	o, err := h.orders.CancelOrder(ctx, id, reason)
	if err != nil {
		return nil, h.problem(ctx, err, "body")
	}
	return &OrderOutput{Body: toBody(o)}, nil
}

// problem traduit une erreur du domaine ou de l'application en réponse
// RFC 9457. location préfixe les champs invalides (« body », « query »).
// Une erreur inattendue n'est jamais renvoyée telle quelle : elle peut
// contenir des détails internes. Elle est journalisée, et le client reçoit un
// message générique.
func (h handlers) problem(ctx context.Context, err error, location string) error {
	var verr *order.ValidationError
	switch {
	case errors.As(err, &verr):
		details := make([]error, len(verr.Fields))
		for i, f := range verr.Fields {
			loc := f.Field
			if location != "" {
				loc = location + "." + f.Field
			}
			details[i] = &huma.ErrorDetail{Location: loc, Message: f.Message}
		}
		return huma.Error422UnprocessableEntity("validation failed", details...)
	case errors.Is(err, app.ErrNotFound):
		return huma.Error404NotFound("order not found")
	case errors.Is(err, order.ErrAlreadyCancelled):
		return huma.Error409Conflict("order already cancelled")
	case errors.Is(err, app.ErrConflict):
		return huma.Error409Conflict("order modified concurrently, retry")
	default:
		h.log.ErrorContext(ctx, "request failed", "err", err)
		return huma.Error500InternalServerError("internal error")
	}
}

// Le curseur est l'identifiant de la dernière commande d'une page, encodé :
// opaque pour le client, qui ne doit pas en dépendre.
func encodeCursor(id order.ID) string { return base64.RawURLEncoding.EncodeToString(id[:]) }

func decodeCursor(s string) (order.ID, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != 16 {
		return order.ID{}, false
	}
	return order.ID(b), true
}
