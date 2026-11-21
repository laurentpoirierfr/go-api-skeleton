# États d'une commande

```mermaid
stateDiagram-v2
    [*] --> placed: order.Place()<br/>version 1<br/>→ order.created.v1
    placed --> cancelled: Cancel()<br/>version + 1<br/>→ order.cancelled.v1
    cancelled --> cancelled: Cancel()<br/>ErrAlreadyCancelled (409)
    cancelled --> [*]
```

Chaque modification incrémente la **version**. La base n'accepte la mise à
jour que si elle contient encore la version précédente : deux annulations
simultanées ne peuvent pas réussir toutes les deux (verrou optimiste).

```mermaid
sequenceDiagram
    participant A as requête A
    participant B as requête B
    participant DB as PostgreSQL
    A->>DB: lit la commande (version 1)
    B->>DB: lit la commande (version 1)
    A->>DB: UPDATE … SET version = 2 WHERE version = 1
    DB-->>A: 1 ligne : succès
    B->>DB: UPDATE … SET version = 2 WHERE version = 1
    DB-->>B: 0 ligne : ErrConflict (409, à réessayer)
```
