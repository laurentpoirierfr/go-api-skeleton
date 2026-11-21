# syntax=docker/dockerfile:1
#
# Une image par binaire, à partir du même Dockerfile :
#
#   docker build --build-arg APP=api    --build-arg VERSION=1.2.0 -t go-api-skeleton-api .
#   docker build --build-arg APP=worker --build-arg VERSION=1.2.0 -t go-api-skeleton-worker .
#
# L'image finale ne contient que le binaire statique et sa configuration par
# défaut : pas de shell, pas de gestionnaire de paquets, un utilisateur non
# root. Les secrets (POSTGRES_URL…) sont passés par l'environnement.

ARG GO_VERSION=1.27

# --- Compilation ---------------------------------------------------------------
FROM docker.io/library/golang:${GO_VERSION} AS build
WORKDIR /src

# Les dépendances d'abord : cette couche ne change que si go.mod ou go.sum
# changent, et le cache des modules est partagé entre les constructions.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG APP=api
ARG VERSION=dev
# CGO_ENABLED=0 : un binaire statique, qui tourne sans libc (distroless/static).
# -trimpath et -s -w : des chemins neutres et un binaire sans symboles de
# débogage, plus petit.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/laurentpoirierfr/go-api-skeleton/internal/platform/version.Version=${VERSION}" \
      -o /out/app ./cmd/${APP}

# --- Exécution -----------------------------------------------------------------
FROM gcr.io/distroless/static-debian13:nonroot
ARG APP=api
ARG PORT=8080

WORKDIR /app
COPY --from=build /out/app /app/app
# La configuration par défaut, cherchée dans configs/<app>.yaml relativement au
# répertoire de travail ; chaque clé reste surchargeable par l'environnement.
COPY configs/${APP}.yaml /app/configs/${APP}.yaml

# En conteneur, le journal est en JSON, pour les collecteurs.
ENV LOG_FORMAT=json
EXPOSE ${PORT}
USER nonroot:nonroot
ENTRYPOINT ["/app/app"]
