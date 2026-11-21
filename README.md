# go-api-skeleton

Squelette d'API Go : HTTP, PostgreSQL, outbox et NATS JetStream.

## Tests

| Niveau | Ce qui est testé | Commande |
|--------|------------------|----------|
| unitaires | domaine, cas d'usage (adaptateurs en mémoire), configuration, middlewares | `make check` (avec `-short`) |
| intégration | adaptateurs face à un vrai PostgreSQL, NATS et Redpanda (Testcontainers) : suite de contrat des dépôts, outbox, consommateur, propagation des traces | `make test` |
| bout en bout | les binaires compilés, lancés comme des processus et configurés par l'environnement : la vie d'une commande par l'API HTTP, jusqu'à la notification du worker, avec chaque broker | `make e2e` |

Les tests d'intégration et de bout en bout ont besoin de Docker, ou de Podman
(son socket utilisateur : `systemctl --user enable --now podman.socket`).

## Schémas

Le dossier [`assets/`](assets/README.md) décrit le squelette en schémas : architecture
hexagonale, parcours d'une commande jusqu'au worker, cycle de vie d'un message,
base de données, exécution, démarrage et arrêt. `make diagrams` vérifie qu'ils
se rendent sans erreur.

## Développement

```bash
make help    # les tâches disponibles
make check   # formatage, analyse statique et tests
make lint    # golangci-lint (dans un conteneur)
make vuln    # vulnérabilités des dépendances
```

## Configuration

Chaque binaire lit, par ordre de priorité croissante :

1. les valeurs par défaut, documentées dans
   [`configs/reference.yaml`](configs/reference.yaml) (générée à partir du code) ;
2. son fichier YAML : `configs/api.yaml` ou `configs/worker.yaml`, ou celui de
   `-config` (ou `CONFIG_FILE`) ;
3. les variables d'environnement : la clé en majuscules, les points remplacés
   par `_` (`messaging.nats.url` → `MESSAGING_NATS_URL`).

```bash
go run ./cmd/api -print-config   # configuration effective, source de chaque valeur, secrets masqués
make config-reference            # régénère configs/reference.yaml après un changement de Config
```

Les fichiers de `configs/` ne contiennent que ce qui diffère des valeurs par
défaut en développement. Une configuration invalide arrête le binaire au
démarrage, avec la liste complète des erreurs.

## Messagerie : NATS ou Redpanda

Le broker se choisit par la configuration (`messaging.broker`, ou
`MESSAGING_BROKER`) : `nats` (NATS JetStream, par défaut) ou `redpanda` (ou
tout broker Kafka). Le domaine, l'outbox, le format des messages et
l'idempotence du worker sont communs ; seul l'adaptateur change.

```bash
make up                          # PostgreSQL, collecteur OpenTelemetry, Jaeger et NATS
make up BROKER=redpanda          # la même chose avec Redpanda (console : http://localhost:8082)
MESSAGING_BROKER=redpanda make run-api
```

Les deux brokers n'offrent pas les mêmes garanties :

| | NATS JetStream | Redpanda (Kafka) |
|-|----------------|------------------|
| Doublons à la publication | éliminés par le broker (`Nats-Msg-Id`) | possibles : l'idempotence du worker les absorbe |
| Relivraison après échec | différée par le broker, sans bloquer les autres messages | émulée par le worker : la partition attend le délai |
| Nombre de livraisons | compté par le broker | compté par le worker (remis à zéro au redémarrage) |
| Ordre | celui du stream | par commande (clé de partition = identifiant de commande) |
| Topologie | streams `ORDERS` et `DLQ` | topics `orders.created`, `orders.cancelled`, `dlq.<consommateur>` |

Les mêmes tests d'intégration tournent contre les deux brokers.

## Observabilité : traces OpenTelemetry

Les deux binaires envoient leurs traces en OTLP au **collecteur
OpenTelemetry** (`deploy/otel-collector.yaml`), qui les relaie vers Jaeger :
changer de backend ne touche que la configuration du collecteur.

```text
api, worker ──OTLP :4318──▶ collecteur ──▶ Jaeger (http://localhost:16686)
```

Une même trace suit une commande de bout en bout : la requête HTTP (nommée par
sa route), chaque requête SQL, la publication de l'événement, puis son
traitement par le worker. Le contexte traverse l'outbox et le broker dans
l'en-tête `traceparent` du message, avec NATS comme avec Redpanda. Chaque ligne
de journal écrite dans une trace porte `trace_id` et `span_id`.

| Clé | Rôle |
|-----|------|
| `telemetry.enabled` | envoyer les traces (activé dans `configs/*.yaml`, désactivé par défaut) |
| `telemetry.endpoint` | URL OTLP HTTP du collecteur |
| `telemetry.sample_ratio` | part des nouvelles traces gardées |

## Fichiers .env d'exemple

`configs/api.env.example` et `configs/worker.env.example` contiennent toutes
les variables d'un binaire, documentées, avec les valeurs du développement
local. Ils sont générés à partir du code (`make config-reference`), et suffisent
seuls à démarrer :

```bash
set -a; . ./configs/api.env.example; set +a; go run ./cmd/api
docker run --env-file configs/worker.env.example go-api-skeleton-worker:dev
```

## Images Docker

Un seul `Dockerfile` construit les deux binaires (`--build-arg APP=api|worker`),
en deux étapes : compilation dans `golang`, puis exécution dans
`distroless/static:nonroot` (une dizaine de Mo, sans shell, non root).

```bash
make images                        # go-api-skeleton-api et -worker, version issue de git
make images VERSION=1.2.0          # une version précise
make up-apps                       # les dépendances et les deux applications (BROKER=redpanda possible)
```

L'image embarque `configs/<app>.yaml` ; chaque clé reste surchargeable par
l'environnement (`POSTGRES_URL`, `MESSAGING_NATS_URL`…), et le journal y est en JSON.

## API HTTP

L'API est décrite par son contrat OpenAPI 3.1, généré à partir du code (Huma)
et versionné dans [`api/openapi.yaml`](api/openapi.yaml) : un test échoue s'il
n'est pas à jour (`make openapi` le régénère). En fonctionnement, l'API sert
aussi `/openapi.json` et une documentation interactive sur `/docs`.

| Méthode | Route | Effet |
|---------|-------|-------|
| `POST` | `/v1/orders` | crée une commande (201, en-tête `Location`) |
| `GET` | `/v1/orders/{id}` | lit une commande |
| `GET` | `/v1/orders?status=&limit=&cursor=` | liste, plus récentes d'abord, pagination par curseur |
| `POST` | `/v1/orders/{id}/cancellation` | annule (409 si déjà annulée) |
| `GET` | `/health/live`, `/health/ready` | sondes |

Les erreurs suivent la RFC 9457 (`application/problem+json`), avec la position
de chaque champ invalide (`body.lines[0].quantity`). Chaque requête porte un
identifiant (`X-Request-ID`, repris ou créé), présent dans le journal.

Organisation :

```text
cmd/       les programmes (un dossier par binaire)
internal/  le code propre à ce module, non importable ailleurs
pkg/       le code réutilisable par d'autres modules (si besoin)
```

Les branches suivent git flow : `develop` pour le travail courant, `main` pour
les versions publiées (`git flow feature start …`, `git flow release start …`).
