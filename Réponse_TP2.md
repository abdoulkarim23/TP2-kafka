# TP2 – Communication entre microservices via Kafka

## 1.1 Architecture du TP

Les microservices ne communiquent plus directement par REST : ils passent par Kafka.

- **commandes** (producteur) : reçoit une nouvelle commande par `POST /commandes` et publie un événement dans le topic `commandes-creees`.
- **Kafka** (broker) : stocke les événements dans le topic `commandes-creees` (2 partitions).
- **clients** (consommateur, groupe `clients-group`) : lit les événements, affiche chaque commande et compte les commandes par client.
- **statistiques** (consommateur, groupe `statistiques-group`) : calcule le nombre de commandes par produit et le chiffre d'affaires par mois.
- **kafka-ui** : interface web (http://localhost:8090) pour observer les topics, les messages et les groupes de consommateurs.

```
  Utilisateur ── POST /commandes ──►  commandes  (producteur, port 8081)
                                          │
                                          │ publie un événement JSON
                                          ▼
                          ┌───────────────────────────────┐
                          │ Kafka (broker)                │
                          │ topic : commandes-creees      │
                          │ [partition 0] [partition 1]   │
                          └───────┬───────────────┬───────┘
                                  │ consomme      │ consomme
                                  ▼               ▼
                              clients        statistiques
                            (port 8080)      (port 8082)
                          clients-group   statistiques-group
```

Tous les conteneurs sont définis dans le même `docker-compose.yml` et partagent donc le même réseau Docker. Les microservices joignent Kafka par son nom de conteneur : `kafka:9092`.

## 1.2 Principe d'un événement

Lorsqu'une commande est créée, `commandes` publie dans le topic `commandes-creees` un événement JSON :

```json
{
  "commande_id": 1,
  "client_id": 2,
  "produit": "PC",
  "montant": 1200,
  "date": "2026-10-11"
}
```

Le champ `date` est ajouté pour permettre plus tard le calcul du chiffre d'affaires par mois.

## 1.3 Installation de Kafka avec Docker

Kafka est lancé en mode KRaft (sans Zookeeper) avec l'image `apache/kafka:3.9.0`, défini dans `docker-compose.yml` :

- `kafka:9092` : adresse utilisée par les autres conteneurs ;
- `localhost:9094` : adresse accessible depuis la machine hôte ;
- `KAFKA_NUM_PARTITIONS: 2` : chaque topic a 2 partitions.

Commandes utilisées :

```bash
docker compose up -d
docker ps
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic commandes-creees --partitions 2 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 1.4 Producteur Kafka : microservice commandes

| Élément | Choix |
|---|---|
| Dépendances | `flask` (API REST), `confluent-kafka` (client Kafka) |
| Connexion au broker | `Producer({"bootstrap.servers": "kafka:9092"})` |
| Topic | `commandes-creees` |
| Message | JSON : `commande_id`, `client_id`, `produit`, `montant`, `date` |
| Envoi | dans la route `POST /commandes`, avec `producer.produce(...)` puis `producer.flush()` |

Le message est envoyé avec la clé `client_id` : toutes les commandes d'un même client vont dans la même partition.

Publication d'une commande :

```bash
curl -X POST http://localhost:8081/commandes -H "Content-Type: application/json" -d '{"client_id": 2, "produit": "PC", "montant": 1200}'
```

Vérification avec un consommateur console :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic commandes-creees --from-beginning
```

Les logs de `commandes` (`docker logs commandes`) confirment l'envoi : topic, partition et offset de chaque message.

## 1.5 Consommateur Kafka : microservice clients

`clients` s'abonne au topic `commandes-creees` avec :

- `group.id = "clients-group"` ;
- `auto.offset.reset = "earliest"` : au premier démarrage du groupe, la lecture commence au début du topic.

Pour chaque événement reçu, il affiche le client, le produit et le montant, puis le nombre total de commandes par client :

```
[CLIENTS] Commande reçue (partition=1, offset=0) : client=Alex, produit=PC, montant=1200
[CLIENTS]   -> Total commandes par client : {'Alex': 1}
```

Les totaux sont aussi consultables par l'API :

```bash
curl http://localhost:8080/clients/statistiques
```

**Observation :** au démarrage, `clients` a consommé les commandes envoyées avant son lancement. `commandes` et `clients` ne s'appellent jamais directement : tous les échanges passent par Kafka.

## 1.6 Arrêter un microservice

Commandes utilisées :

```bash
docker stop clients
# création de 3 commandes avec curl -X POST ...
docker start clients
docker logs -f clients
```

**Les événements produits pendant l'arrêt de clients ont-ils été perdus ?**
Non. Pendant l'arrêt, `commandes` continue de fonctionner normalement et Kafka conserve les messages dans le topic.

**Messages consommés par clients au redémarrage :**
`clients` consomme les commandes créées pendant son arrêt, et uniquement celles-là. Kafka mémorise pour le groupe `clients-group` le dernier offset lu, et le consommateur reprend à partir de cette position.

Contrairement au TP1, où l'arrêt de `clients` provoquait une erreur 503, la communication asynchrone rend les services indépendants.

## 1.7 Deuxième consommateur : microservice statistiques

Un troisième microservice, `statistiques` (port 8082), consomme lui aussi le topic `commandes-creees`, avec son propre groupe `statistiques-group`. Il calcule :

- le **nombre total de commandes par produit** ;
- le **chiffre d'affaires total par mois**.

**Modification des événements :** pour calculer le chiffre d'affaires par mois, il faut connaître la date de chaque commande. Un champ `date` (format `AAAA-MM-JJ`) a donc été ajouté à l'événement publié par `commandes`. Le mois correspond aux 7 premiers caractères (`"2026-10-11"` → `"2026-10"`).

Pour chaque événement reçu, `statistiques` met à jour ses compteurs et les affiche :

```
[STATS] Commande reçue (partition=1, offset=3) : produit=PC, montant=1200, mois=2026-10
[STATS]   -> Commandes par produit : {'PC': 2, 'Casque': 1, 'Livre': 1}
[STATS]   -> CA par mois : {'2026-10': 2510.0}
```

Endpoints REST :

| Méthode | URL | Fonction |
|---|---|---|
| GET | `/statistiques` | Toutes les statistiques |
| GET | `/statistiques/produits/<produit>` | Nombre de commandes pour un produit |
| GET | `/statistiques/ca/<mois>` | Chiffre d'affaires pour un mois (`AAAA-MM`) |

Tests :

```bash
curl -X POST http://localhost:8081/commandes -H "Content-Type: application/json" -d '{"client_id": 1, "produit": "PC", "montant": 1200, "date": "2026-09-15"}'
curl http://localhost:8082/statistiques/produits/PC
curl http://localhost:8082/statistiques/ca/2026-09
curl http://localhost:8082/statistiques/ca/2026-10
```

**Observation :** au premier démarrage, `statistiques` a lu toutes les commandes déjà présentes dans le topic, y compris celles publiées avant sa création, car son groupe était nouveau (`auto.offset.reset = earliest`).

## 2. Kafka et groupes de consommateurs

### Deux consommateurs dans le même groupe : groupe-A

Deux instances du microservice `clients` ont été lancées avec `group_id = "groupe-A"` :

```bash
docker run -d --name conso-a1 --network tp2-kafka_default -e GROUP_ID=groupe-A tp2-kafka-clients
docker run -d --name conso-a2 --network tp2-kafka_default -e GROUP_ID=groupe-A tp2-kafka-clients
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group groupe-A
```

**Observation de la distribution :**

- Kafka a attribué **une partition à chaque consommateur** : l'un lit la partition 0, l'autre la partition 1.
- Dans nos tests, tous les messages se sont retrouvés dans la partition 1 (la partition est choisie par un hachage de la clé `client_id`, et les clés utilisées tombaient toutes dans la même partition). Le consommateur de la partition 1 a donc reçu tous les messages, et celui de la partition 0 aucun.
- Aucun message n'a été lu par les deux consommateurs : chaque message est traité par **un seul** consommateur du groupe.

### Deux consommateurs dans des groupes différents

`clients` utilise `group_id = "clients-group"` et `statistiques` utilise `group_id = "statistiques-group"`.

Après l'envoi d'une seule commande :

```bash
curl -X POST http://localhost:8081/commandes -H "Content-Type: application/json" -d '{"client_id": 1, "produit": "Casque", "montant": 80}'
docker logs --tail 4 clients
docker logs --tail 6 statistiques
```

La même commande (partition 1, offset 14) apparaît dans les logs de `clients` **et** dans ceux de `statistiques`.

### Quelle différence observez-vous entre les deux configurations ?

| | Même groupe (`groupe-A`) | Groupes différents (`clients-group`, `statistiques-group`) |
|---|---|---|
| Réception | Les consommateurs **se partagent** les messages | **Chaque groupe reçoit tous** les messages |
| Un message est lu par | un seul consommateur du groupe | un consommateur dans chaque groupe |
| Répartition | par partition (une partition = un seul consommateur du groupe) | chaque groupe garde son propre offset |
| Utilité | répartir la charge entre plusieurs instances d'un même service | permettre à plusieurs services différents de réagir au même événement |

Avec 2 partitions, un groupe peut avoir au maximum 2 consommateurs actifs : le nombre de partitions limite le parallélisme au sein d'un groupe.