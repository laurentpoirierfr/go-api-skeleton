# Exécution

```mermaid
flowchart LR
    client(["Client HTTP"]) -->|":8080 /v1/orders"| api
    orch(["Orchestrateur"]) -.->|"/health/live, /health/ready"| api
    orch -.->|":8081 /health/*"| worker

    subgraph apps["Applications (images distroless, non root)"]
        api["api<br/>HTTP + forwarder"]
        worker["worker<br/>consommateur"]
    end

    subgraph deps["Dépendances (compose.yaml)"]
        pg[("PostgreSQL 17<br/>:5432")]
        nats[["NATS 2.12 JetStream<br/>:4222, supervision :8222"]]
        jaeger["Jaeger<br/>OTLP :4317/:4318<br/>UI :16686"]
    end

    api -->|"transactions,<br/>outbox"| pg
    api -->|"publie orders.*"| nats
    nats -->|"livre orders.*"| worker
    worker -->|"inbox, notifications"| pg
    api -.->|"traces"| jaeger
    worker -.->|"traces"| jaeger
```

| Commande | Effet |
|----------|-------|
| `make up` | démarre PostgreSQL, NATS et Jaeger, et attend qu'ils soient prêts |
| `make run-api`, `make run-worker` | lance un binaire avec `configs/<binaire>.yaml` |
| `docker compose --profile apps up` | lance aussi les deux applications, depuis le Dockerfile |
| `make images` | construit les deux images |

**La configuration** : valeurs par défaut du code, puis `configs/<binaire>.yaml`,
puis variables d'environnement (`HTTP_ADDRESS` surcharge `http.address`).

| Sonde | Répond 200 si… | Sinon |
|-------|----------------|-------|
| `/health/live` | le processus répond | l'orchestrateur le redémarre |
| `/health/ready` | PostgreSQL et NATS répondent, et l'arrêt n'a pas commencé | 503 : retiré du trafic, sans redémarrage |
