# Créer une commande : de la requête HTTP au worker

La commande et son événement sont écrits **dans la même transaction** : un
événement n'est publié que si la commande existe, et il n'est jamais perdu.
La publication est ensuite **asynchrone** : l'API répond sans attendre NATS.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as api<br/>httpapi (Huma)
    participant Svc as OrderService
    participant Dom as domain/order
    participant DB as PostgreSQL
    participant Fwd as forwarder<br/>(dans api)
    participant JS as NATS JetStream<br/>stream ORDERS
    participant W as worker

    Client->>API: POST /v1/orders
    API->>API: validation du schéma (422 si invalide)
    API->>Svc: PlaceOrder(cmd)
    Svc->>Dom: order.Place(...)
    Dom-->>Svc: Order + OrderPlaced (ou ValidationError → 422)

    rect rgb(235, 245, 255)
    note over Svc,DB: une seule transaction (UnitOfWork)
    Svc->>DB: INSERT orders, order_lines (COPY)
    Svc->>DB: INSERT watermill_outbox (événement enveloppé)
    Svc->>DB: COMMIT
    end

    Svc-->>API: Order
    API-->>Client: 201 Created + Location

    loop toutes les 200 ms
        Fwd->>DB: SELECT … WHERE acked = false FOR UPDATE
    end
    Fwd->>JS: publie orders.created<br/>Nats-Msg-Id = ce-id
    JS-->>Fwd: accusé de réception
    Fwd->>DB: DELETE du message (DeleteOnAck)

    JS->>W: livraison (consommateur durable)
    W->>DB: transaction : processed_messages + notifications
    W-->>JS: Ack
```

**Si NATS est indisponible** : les étapes 1 à 10 fonctionnent normalement, le
client reçoit 201 ; l'événement attend dans `watermill_outbox`, et le
forwarder le publie au retour de NATS. `/health/ready` signale la coupure.

**Si l'API s'arrête entre la publication et l'effacement** : le message est
publié une seconde fois au redémarrage ; JetStream l'élimine, car il connaît
déjà son `Nats-Msg-Id` (fenêtre de déduplication de 2 minutes).
