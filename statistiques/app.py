import json
import os
import threading
from collections import defaultdict

from confluent_kafka import Consumer
from flask import Flask, jsonify

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.environ.get("GROUP_ID", "statistiques-group")
TOPIC = "commandes-creees"

nb_par_produit = defaultdict(int)    # {produit: nombre de commandes}
ca_par_mois = defaultdict(float)     # {"AAAA-MM": chiffre d'affaires}
verrou = threading.Lock()


def consommer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"[STATS] Abonné au topic '{TOPIC}' (groupe '{GROUP_ID}')", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[STATS] Erreur Kafka : {msg.error()}", flush=True)
            continue

        evt = json.loads(msg.value().decode("utf-8"))
        mois = evt["date"][:7]  # "2026-10-11" -> "2026-10"

        with verrou:
            nb_par_produit[evt["produit"]] += 1
            ca_par_mois[mois] += evt["montant"]
            print(f"[STATS] Commande reçue (partition={msg.partition()}, offset={msg.offset()}) : "
                  f"produit={evt['produit']}, montant={evt['montant']}, mois={mois}", flush=True)
            print(f"[STATS]   -> Commandes par produit : {dict(nb_par_produit)}", flush=True)
            print(f"[STATS]   -> CA par mois : {dict(ca_par_mois)}", flush=True)


@app.route("/statistiques", methods=["GET"])
def toutes():
    with verrou:
        return jsonify({"commandes_par_produit": nb_par_produit,
                        "ca_par_mois": ca_par_mois}), 200


@app.route("/statistiques/produits/<produit>", methods=["GET"])
def par_produit(produit):
    with verrou:
        return jsonify({"produit": produit,
                        "nombre_commandes": nb_par_produit.get(produit, 0)}), 200


@app.route("/statistiques/ca/<mois>", methods=["GET"])
def ca_mois(mois):
    with verrou:
        return jsonify({"mois": mois,
                        "chiffre_affaires": ca_par_mois.get(mois, 0)}), 200


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
