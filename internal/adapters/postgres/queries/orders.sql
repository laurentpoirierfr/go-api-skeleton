-- name: CreateOrder :exec
INSERT INTO orders (id, customer, total_cents, status, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: CreateOrderLines :copyfrom
INSERT INTO order_lines (order_id, position, product, quantity, unit_price_cents)
VALUES ($1, $2, $3, $4, $5);

-- name: GetOrder :one
SELECT * FROM orders WHERE id = $1;

-- name: ListOrders :many
-- Les commandes les plus récentes d'abord ; status et before sont facultatifs.
SELECT * FROM orders
WHERE (sqlc.narg(status)::text IS NULL OR status = sqlc.narg(status))
  AND (sqlc.narg(before)::uuid IS NULL OR id < sqlc.narg(before))
ORDER BY id DESC
LIMIT sqlc.arg(row_limit);

-- name: ListOrderLines :many
-- Les lignes de plusieurs commandes en une requête (pas de N+1).
SELECT * FROM order_lines
WHERE order_id = ANY (sqlc.arg(order_ids)::uuid[])
ORDER BY order_id, position;

-- name: UpdateOrder :execrows
-- Verrou optimiste : la mise à jour n'a lieu que si la base contient encore
-- la version précédente. Les lignes d'une commande ne changent jamais.
UPDATE orders
SET status = sqlc.arg(status), version = sqlc.arg(version), updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id) AND version = sqlc.arg(expected_version);
