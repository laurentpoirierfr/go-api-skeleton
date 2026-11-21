# go-api-skeleton

Squelette d'API Go : HTTP, PostgreSQL, outbox et NATS JetStream.

## Développement

```bash
make help    # les tâches disponibles
make check   # formatage, analyse statique et tests
make lint    # golangci-lint (dans un conteneur)
make vuln    # vulnérabilités des dépendances
```

## Images Docker

Un seul `Dockerfile` construit les deux binaires (`--build-arg APP=api|worker`),
en deux étapes : compilation dans `golang`, puis exécution dans
`distroless/static:nonroot` (une dizaine de Mo, sans shell, non root).

```bash
make images                        # go-api-skeleton-api et -worker, version issue de git
make images VERSION=1.2.0          # une version précise
docker compose --profile apps up   # les dépendances et les deux applications
```

L'image embarque `configs/<app>.yaml` ; chaque clé reste surchargeable par
l'environnement (`POSTGRES_URL`, `NATS_URL`…), et le journal y est en JSON.

Organisation :

```text
cmd/       les programmes (un dossier par binaire)
internal/  le code propre à ce module, non importable ailleurs
pkg/       le code réutilisable par d'autres modules (si besoin)
```

Les branches suivent git flow : `develop` pour le travail courant, `main` pour
les versions publiées (`git flow feature start …`, `git flow release start …`).
