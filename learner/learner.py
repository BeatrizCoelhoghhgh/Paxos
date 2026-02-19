#learner.py
#IMPORTAÇÕES
from flask import Flask, request, jsonify
import requests
from collections import defaultdict
import os

from prometheus_client import Counter, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

#cria um servidor web pro Learner (para receber as requisocoes http)
app = Flask(__name__)

#Métricas 
#quantas propostas tiveram sucesso 
COMMIT_TOTAL = Counter('paxos_commit_total', 'Total de propostas COMMITTED (Quorum atingido)')
#conta quantas vezes o Learner avisou o cliente
NOTIFICATION_SENT = Counter('paxos_client_notification_sent_total', 'Total de notificações enviadas ao Cliente')

#cada proposal_id vai quardar: quantos fotos sim, quantos não, o valor que está sendo decidido e se o cliente já foi notificado.
proposal_votes = defaultdict(lambda: {"yes": 0, "no": 0, "transaction": None, "notified": False})

#precisa de pelo menos 2 votos iguais 
QUORUM = 2  

#esse endpoint é chamado pelo Acceptors 
#é aqui que o Learner recebe os votos
@app.post("/learn")
def learn():
    data = request.get_json()
    proposal_id = data.get("proposal_id")
    accepted = data.get("accepted")
    transaction = data.get("transaction")

    #busca registro dessa proposta
    entry = proposal_votes[proposal_id]
    entry["transaction"] = transaction

    #conta os votos
    if accepted:
        entry["yes"] += 1
    else:
        entry["no"] += 1

    #verifica se houve commit
    if entry["yes"] >= QUORUM and not entry["notified"]: 
        COMMIT_TOTAL.inc()
        notify_client(transaction, True, proposal_id)
        entry["notified"] = True # Marca como notificado!
        return jsonify({"status": "committed"}), 200

    # 2. Faz o mesmo para rejeição
    if entry["no"] >= QUORUM and not entry["notified"]:
        notify_client(transaction, False, proposal_id)
        entry["notified"] = True # Marca como notificado!
        return jsonify({"status": "rejected"}), 200

    return jsonify({"status": "pending"}), 200

#responsavel por fechar o ciclo do Paxos, aqui o cliente vai receber o resultado
def notify_client(transaction, committed, proposal_id):
   
    try:
        client_id = transaction.get("client_id")
        request_id = transaction.get("request_id")
        if not client_id or not request_id:
           
            print(f"[LEARNER] ERROR: Missing client_id or request_id in transaction: {transaction}", flush=True)
            return
    except Exception:
        return

    payload = {
        "request_id": request_id,
        "result": "COMMITTED" if committed else "REJECTED",
        "proposal_id": proposal_id
    }
    try:
        NOTIFICATION_SENT.inc()
        requests.post(f"http://{client_id}:5000/commit", json=payload, timeout=2)
        print(f"[LEARNER] Notified client {client_id} for request {request_id} -> {payload['result']}", flush=True)
    except Exception as e:
        print(f"[LEARNER] Failed notifying client {client_id}: {e}", flush=True)

@app.get("/")
def root():
    return "LEARNER OK"

app_dispatcher = DispatcherMiddleware(app, {
    '/metrics': make_wsgi_app()
})

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    
    print("LEARNER starting on port 8200. Paxos endpoints and /metrics exposed.", flush=True)

    run_simple('0.0.0.0', 8200, app_dispatcher)
