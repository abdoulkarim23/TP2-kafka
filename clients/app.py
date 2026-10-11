import json
import os
import threading
from collections import defaultdict

from confluent_kafka import Consumer
from flask import Flask, jsonify

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.environ.get("GROUP_ID", "clients-group")
TOPIC = "commandes-creees"

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]

# Nombre total de commandes par client : {client_id: nombre}
nb_commandes = defaultdict(int)
verrou = threading.Lock()


def nom_client(client_id):
    c = next((c for c in clients if c["id"] == client_id), None)
    return c["nom"] if c else f"Client inconnu ({client_id})"


def consommer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"[CLIENTS] Abonné au topic '{TOPIC}' (groupe '{GROUP_ID}')", flush=True)

    while True:
        msg = consumer.poll(1.0)  # attend un message pendant 1 s max
        if msg is None:
            continue
        if msg.error():
            print(f"[CLIENTS] Erreur Kafka : {msg.error()}", flush=True)
            continue

        evt = json.loads(msg.value().decode("utf-8"))
        cid = evt["client_id"]

        with verrou:
            nb_commandes[cid] += 1
            totaux = {nom_client(k): v for k, v in sorted(nb_commandes.items())}

        print(f"[CLIENTS] Commande reçue (partition={msg.partition()}, offset={msg.offset()}) : "
              f"client={nom_client(cid)}, produit={evt['produit']}, montant={evt['montant']}",
              flush=True)
        print(f"[CLIENTS]   -> Total commandes par client : {totaux}", flush=True)


@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients), 200


@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    c = next((c for c in clients if c["id"] == client_id), None)
    if c is None:
        return jsonify({"erreur": f"Client {client_id} introuvable"}), 404
    return jsonify(c), 200


@app.route("/clients/statistiques", methods=["GET"])
def statistiques():
    with verrou:
        return jsonify({nom_client(k): v for k, v in nb_commandes.items()}), 200


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
