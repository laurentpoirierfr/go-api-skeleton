# go-api-skeleton : tâches courantes. « make help » les liste.
GOLANGCI_IMAGE := docker.io/golangci/golangci-lint:v2.15.0
# Podman sans root : keep-id garde l'identité de l'utilisateur dans le conteneur.
CONTAINER_USER := $(if $(shell docker --version 2>/dev/null | grep -i podman),--userns=keep-id,--user $(shell id -u):$(shell id -g))

.PHONY: help build test check lint vuln cover clean up down run-api run-worker images generate

## help : liste les cibles
help:
	@grep -h '^## ' $(MAKEFILE_LIST) | sed 's/^## //'

## build : compile les programmes dans bin/
build:
	go build -o bin/ ./cmd/...

# Testcontainers parle au démon Docker ; avec Podman, à son socket (ryuk, le
# nettoyeur de Testcontainers, demande des privilèges : les tests arrêtent
# eux-mêmes leurs conteneurs).
PODMAN_SOCK := /run/user/$(shell id -u)/podman/podman.sock
TC_ENV := $(if $(wildcard $(PODMAN_SOCK)),DOCKER_HOST=unix://$(PODMAN_SOCK) TESTCONTAINERS_RYUK_DISABLED=true)

## test : lance tous les tests, y compris d'intégration (Testcontainers)
test:
	$(TC_ENV) go test ./...

# La version des images : le dernier tag, et le commit s'il y en a d'autres depuis.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE ?= go-api-skeleton

## images : construit les images de l'API et du worker (IMAGE, VERSION)
images:
	docker build --build-arg APP=api --build-arg PORT=8080 --build-arg VERSION=$(VERSION) -t $(IMAGE)-api:$(VERSION) .
	docker build --build-arg APP=worker --build-arg PORT=8081 --build-arg VERSION=$(VERSION) -t $(IMAGE)-worker:$(VERSION) .

SQLC_IMAGE := docker.io/sqlc/sqlc:1.31.1

## generate : régénère le code (sqlc)
generate:
	docker run --rm --volume "$(CURDIR):/src" --workdir /src $(CONTAINER_USER) $(SQLC_IMAGE) generate

## up : démarre les dépendances (PostgreSQL, NATS, Jaeger) et attend qu'elles soient prêtes
up:
	docker compose up -d --wait

## down : arrête les dépendances et supprime leurs données
down:
	docker compose down -v

## run-api : lance l'API (après make up)
run-api:
	go run ./cmd/api -config configs/api.yaml

## run-worker : lance le worker (après make up)
run-worker:
	go run ./cmd/worker -config configs/worker.yaml

## check : formatage, analyse statique et tests (avant chaque commit)
check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -short ./...

## lint : golangci-lint, dans son conteneur (rien à installer)
lint:
	docker run --rm --volume "$(CURDIR):/app" --workdir /app $(CONTAINER_USER) -e HOME=/tmp \
		-e GOFLAGS=-buildvcs=false $(GOLANGCI_IMAGE) golangci-lint run ./...

## vuln : vulnérabilités connues des dépendances
vuln:
	go tool govulncheck ./...

## cover : couverture des tests, en HTML
cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

## clean : supprime les fichiers produits
clean:
	rm -rf bin coverage.out coverage.html
