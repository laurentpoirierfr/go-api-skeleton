-- Les commandes et leurs lignes. Les identifiants sont des UUID v7 : l'ordre
-- des UUID (octet par octet, celui de PostgreSQL) est l'ordre de création.
CREATE TABLE orders (
    id          uuid        PRIMARY KEY,
    customer    text        NOT NULL,
    total_cents bigint      NOT NULL CHECK (total_cents > 0),
    status      text        NOT NULL CHECK (status IN ('placed', 'cancelled')),
    version     integer     NOT NULL CHECK (version > 0),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

-- La liste filtrée par statut, de la plus récente à la plus ancienne.
CREATE INDEX orders_status_id_idx ON orders (status, id DESC);

CREATE TABLE order_lines (
    order_id         uuid    NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    position         integer NOT NULL,
    product          text    NOT NULL,
    quantity         integer NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint  NOT NULL CHECK (unit_price_cents > 0),
    PRIMARY KEY (order_id, position)
);

-- La boîte de réception : un message déjà traité par un consommateur n'est
-- pas traité une seconde fois (idempotence).
CREATE TABLE processed_messages (
    consumer     text        NOT NULL,
    message_id   text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Les notifications envoyées par le worker. Pas de clé étrangère vers orders :
-- dans un vrai système, le worker aurait souvent sa propre base.
CREATE TABLE notifications (
    id         uuid        PRIMARY KEY,
    order_id   uuid        NOT NULL,
    kind       text        NOT NULL,
    recipient  text        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX notifications_order_id_idx ON notifications (order_id);
