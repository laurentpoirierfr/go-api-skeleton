# Architecture hexagonale

Le domaine et les cas d'usage ne dépendent d'aucune technologie. Ils déclarent
des **ports** (des interfaces Go) ; les **adaptateurs** les implémentent, et les
binaires (`cmd/`) assemblent le tout à la main, sans conteneur d'injection.

```mermaid
flowchart LR
    subgraph entrees["Adaptateurs d'entrée"]
        direction TB
        http["httpapi<br/>Huma · OpenAPI 3.1 · RFC 9457"]
        consumer["consommateur<br/>Router Watermill (worker)"]
    end

    subgraph coeur["Cœur"]
        direction TB
        app["app<br/>cas d'usage"]
        domain["domain/order<br/>règles et événements"]
        ports{{"ports<br/>UnitOfWork · OrderRepository<br/>Outbox · Inbox · NotificationRepository"}}
        app --> domain
        app --> ports
    end

    subgraph sorties["Adaptateurs de sortie"]
        direction TB
        pg["postgres<br/>pgx · sqlc · migrations"]
        msg["messaging<br/>outbox · forwarder<br/>NATS JetStream ou Redpanda"]
        mem["memory<br/>tests sans base"]
    end

    http --> app
    consumer --> app
    pg -.-> ports
    msg -.-> ports
    mem -.-> ports
```

Flèche pleine : « appelle ». Flèche pointillée : « implémente ». Le cœur ne
dépend d'aucune technologie : ni base, ni HTTP, ni messagerie.

**La règle des dépendances** : les flèches pointent vers le cœur. `domain` ne
connaît rien ; `app` ne connaît que `domain` ; les adaptateurs connaissent
`app` et `domain`, jamais l'inverse. Changer de base, de broker ou de
framework HTTP ne touche que les adaptateurs.

```mermaid
flowchart TB
    cmd["cmd/api, cmd/worker<br/>assemblage"] --> adapters["internal/adapters/*"]
    cmd --> platform["internal/platform/*<br/>config (Viper) · logger (slog) · health<br/>runner · httpmw · version"]
    adapters --> app["internal/app"]
    app --> domain["internal/domain/order"]
    adapters --> domain
```
