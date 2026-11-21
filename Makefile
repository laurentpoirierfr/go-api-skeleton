# go-api-skeleton : tâches courantes. « make help » les liste.
GOLANGCI_IMAGE := docker.io/golangci/golangci-lint:v2.15.0
# Podman sans root : keep-id garde l'identité de l'utilisateur dans le conteneur.
CONTAINER_USER := $(if $(shell docker --version 2>/dev/null | grep -i podman),--userns=keep-id,--user $(shell id -u):$(shell id -g))

.PHONY: e2e up-apps help build test check lint vuln cover clean up down run-api run-worker images generate openapi diagrams config-reference

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

## config-reference : régénère configs/reference.yaml et les .env d'exemple à partir du code
config-reference:
	UPDATE_CONFIG_REFERENCE=1 go test -count=1 -run 'TestReferenceUpToDate|TestEnvExamples' ./internal/platform/config

## openapi : régénère le contrat api/openapi.yaml à partir du code
openapi:
	UPDATE_OPENAPI=1 go test -count=1 -run TestOpenAPISpec ./internal/adapters/httpapi

MERMAID_IMAGE := docker.io/minlag/mermaid-cli:11.12.0

## diagrams : vérifie que les schémas Mermaid d'assets/ se rendent sans erreur
diagrams:
	@for f in assets/*.md; do \
		echo "$$f"; \
		docker run --rm --volume "$(CURDIR)/assets:/data:ro" $(MERMAID_IMAGE) \
			-i "/data/$$(basename $$f)" -o "/tmp/$$(basename $$f)" -q || exit 1; \
		done

# Le broker des événements : nats (par défaut) ou redpanda.
BROKER ?= nats

## up : démarre PostgreSQL, Jaeger et le broker (BROKER=nats ou redpanda), et attend qu'ils soient prêts
up:
	BROKER=$(BROKER) docker compose --profile $(BROKER) up -d --wait

## up-apps : comme up, plus l'api et le worker construits depuis le Dockerfile
up-apps:
	BROKER=$(BROKER) docker compose --profile $(BROKER) --profile apps up -d --build --wait

## down : arrête tout (quel que soit le broker) et supprime les données
down:
	docker compose --profile nats --profile redpanda --profile apps down -v

## run-api : lance l'API (après make up)
run-api:
	go run ./cmd/api -config configs/api.yaml

## run-worker : lance le worker (après make up)
run-worker:
	go run ./cmd/worker -config configs/worker.yaml

## e2e : teste les binaires compilés, face à PostgreSQL et à chaque broker (Testcontainers)
e2e:
	$(TC_ENV) go test -count=1 -v ./test/e2e

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
