package httpapi

import (
	"time"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/domain/order"
)

// Les contraintes des étiquettes (minimum, maxLength…) reprennent celles du
// domaine : elles documentent le contrat et rejettent tôt les demandes
// mal formées. Le domaine reste la source de vérité, et valide à nouveau.

type LineInput struct {
	Product        string `json:"product" minLength:"1" maxLength:"100" doc:"Product name" example:"Gopher plush"`
	Quantity       int    `json:"quantity" minimum:"1" maximum:"1000" example:"2"`
	UnitPriceCents int64  `json:"unit_price_cents" minimum:"1" maximum:"100000000" doc:"Unit price, in cents" example:"1250"`
}

type PlaceOrderInput struct {
	Body struct {
		Customer string      `json:"customer" format:"email" maxLength:"254" doc:"Customer email address" example:"ada@example.com"`
		Lines    []LineInput `json:"lines" minItems:"1" maxItems:"50"`
	}
}

type OrderIDInput struct {
	ID string `path:"id" format:"uuid" doc:"Order ID"`
}

type ListOrdersInput struct {
	Status string `query:"status" enum:"placed,cancelled" doc:"Only orders with this status"`
	Cursor string `query:"cursor" doc:"Opaque cursor returned as next_cursor by the previous page"`
	Limit  int    `query:"limit" minimum:"1" maximum:"100" default:"20" doc:"Page size"`
}

// CancelOrderInput a un corps facultatif : pour Huma, c'est un pointeur.
type CancelOrderInput struct {
	ID   string `path:"id" format:"uuid" doc:"Order ID"`
	Body *CancelOrderBody
}

type CancelOrderBody struct {
	Reason string `json:"reason,omitempty" maxLength:"500" doc:"Why the order is cancelled" example:"Ordered by mistake"`
}

type LineBody struct {
	Product        string `json:"product"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
	TotalCents     int64  `json:"total_cents"`
}

type OrderBody struct {
	ID         string     `json:"id" format:"uuid"`
	Customer   string     `json:"customer" format:"email"`
	Status     string     `json:"status" enum:"placed,cancelled"`
	TotalCents int64      `json:"total_cents"`
	Lines      []LineBody `json:"lines"`
	Version    int        `json:"version" doc:"Incremented on every change"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type CreatedOrderOutput struct {
	Location string `header:"Location" doc:"URL of the new order"`
	Body     OrderBody
}

type OrderOutput struct {
	Body OrderBody
}

type OrderPageOutput struct {
	Body struct {
		Items      []OrderBody `json:"items"`
		NextCursor string      `json:"next_cursor,omitempty" doc:"Cursor of the next page; absent on the last page"`
	}
}

func toBody(o order.Order) OrderBody {
	lines := o.Lines()
	b := OrderBody{
		ID: o.ID().String(), Customer: o.Customer(), Status: string(o.Status()), TotalCents: int64(o.Total()),
		Lines: make([]LineBody, len(lines)), Version: o.Version(), CreatedAt: o.CreatedAt(), UpdatedAt: o.UpdatedAt(),
	}
	for i, l := range lines {
		b.Lines[i] = LineBody{Product: l.Product, Quantity: l.Quantity, UnitPriceCents: int64(l.UnitPrice), TotalCents: int64(l.Total())}
	}
	return b
}
