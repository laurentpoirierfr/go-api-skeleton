-- L'outbox : les événements écrits dans la même transaction que les données,
-- puis relayés vers NATS JetStream par le forwarder de Watermill.
--
-- La table suit exactement le schéma « file » de watermill-sql
-- (PostgreSQLQueueSchema, topic « outbox ») : le forwarder lit les messages non
-- acquittés et les supprime une fois publiés (DeleteOnAck), la table reste
-- donc petite. Elle est créée ici, par les migrations, et non par Watermill
-- au démarrage : le schéma reste versionné, et l'API n'a pas besoin du droit
-- de créer des tables.
CREATE TABLE watermill_outbox (
    "offset"     SERIAL PRIMARY KEY,
    "uuid"       VARCHAR(36) NOT NULL,
    "payload"    JSON DEFAULT NULL,
    "metadata"   JSON DEFAULT NULL,
    "acked"      BOOLEAN NOT NULL DEFAULT FALSE,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
