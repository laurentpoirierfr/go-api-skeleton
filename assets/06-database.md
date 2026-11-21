# Base de données

Le schéma est créé par les migrations embarquées dans le binaire
(`internal/adapters/postgres/migrations`), et lu par sqlc pour générer les
requêtes typées.

```mermaid
erDiagram
    orders ||--|{ order_lines : contient
    orders {
        uuid id PK "UUID v7, triable dans le temps"
        text customer
        bigint total_cents "montant en centimes"
        text status "placed | cancelled"
        integer version "verrou optimiste"
        timestamptz created_at
        timestamptz updated_at
    }
    order_lines {
        uuid order_id PK, FK
        integer position PK
        text product
        integer quantity
        bigint unit_price_cents
    }
    watermill_outbox {
        serial offset PK
        varchar uuid "identifiant du message"
        json payload "message enveloppé + sujet de destination"
        json metadata
        boolean acked
        timestamptz created_at
    }
    processed_messages {
        text consumer PK
        text message_id PK
        timestamptz processed_at
    }
    notifications {
        uuid id PK
        uuid order_id "sans clé étrangère"
        text kind "type de l'événement"
        text recipient
        timestamptz created_at
    }
```

- `watermill_outbox` est la table de l'outbox : les lignes y restent le temps
  de leur publication (supprimées ensuite).
- `processed_messages` et `notifications` appartiennent au worker. Elles n'ont
  pas de clé étrangère vers `orders` : dans un vrai système, le worker aurait
  souvent sa propre base.
