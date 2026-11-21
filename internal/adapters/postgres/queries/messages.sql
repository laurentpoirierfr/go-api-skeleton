-- name: MarkProcessed :execrows
-- Zéro ligne insérée : le message avait déjà été traité.
INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- name: CreateNotification :exec
INSERT INTO notifications (id, order_id, kind, recipient, created_at)
VALUES ($1, $2, $3, $4, $5);

-- name: ListNotificationsByOrder :many
SELECT * FROM notifications WHERE order_id = $1 ORDER BY created_at, id;
