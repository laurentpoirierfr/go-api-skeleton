# Schémas techniques

Ces schémas décrivent le squelette : son architecture, le parcours d'une
commande et de ses événements, la base de données et l'exécution. Ils sont
écrits en [Mermaid](https://mermaid.js.org) : du texte versionné, affiché
directement par GitHub, et vérifié par `make diagrams`.

| Schéma | Ce qu'il montre |
|--------|-----------------|
| [1. Architecture hexagonale](01-architecture.md) | les couches, les ports, les adaptateurs, et le sens des dépendances |
| [2. Organisation du code](02-layout.md) | le rôle de chaque répertoire |
| [3. Créer une commande](03-place-order.md) | la requête HTTP, la transaction, l'outbox, la publication, la consommation |
| [4. Cycle de vie d'un message](04-message-lifecycle.md) | de l'outbox à l'acquittement : essais, doublons, file des messages morts |
| [5. États d'une commande](05-order-states.md) | les transitions, les événements, la version |
| [6. Base de données](06-database.md) | les tables et leurs relations |
| [7. Exécution](07-runtime.md) | les binaires, les dépendances, les ports, les sondes |
| [8. Démarrage et arrêt](08-lifecycle.md) | l'ordre de démarrage, l'arrêt propre |

