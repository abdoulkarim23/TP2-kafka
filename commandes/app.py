import json
import os
from datetime import datetime

from confluent_kafka import Producer
from flask import Flask, jsonify, request

app = Flask(__name__)

# Connexion au broker : "kafka" = nom du conteneur sur le réseau Docker
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "commandes-creees"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

commandes = []


def rapport_envoi(err, msg):
    """Appelée par Kafka pour confirmer (ou non) la réception du message."""
    if err is not None:
        print(f"[KAFKA] ERREUR d'envoi : {err}", flush=True)
    else:
        print(f"[KAFKA] Envoyé -> topic={msg.topic()} "
              f"partition={msg.partition()} offset={msg.offset()}", flush=True)


@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes), 200


@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json(silent=True) or {}

    manquants = [c for c in ("client_id", "produit", "montant") if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {manquants}"}), 400

    # Structure de l'événement publié dans Kafka
    commande = {
        "commande_id": len(commandes) + 1,
        "client_id": int(data["client_id"]),
        "produit": data["produit"],
        "montant": data["montant"],
        "date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
    }
    commandes.append(commande)

    # Publication : clé = client_id -> même client = même partition
    producer.produce(
        TOPIC,
        key=str(commande["client_id"]),
        value=json.dumps(commande).encode("utf-8"),
        callback=rapport_envoi,
    )
    producer.flush(5)  # attend la confirmation de Kafka (max 5 s)

    print(f"[COMMANDES] Nouvelle commande : {commande}", flush=True)
    return jsonify(commande), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
