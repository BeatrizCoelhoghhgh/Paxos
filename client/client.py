#client.py

#importações
import requests
from flask import Flask, request, jsonify
import threading
import time
import os
import random

#cria servidor http para o cliente (só para receber commits)
app = Flask(__name__)

# Identificador do cliente (docker fornece HOSTNAME)
CLIENT_ID = os.getenv("HOSTNAME", "client")
TARGET_NODE = os.getenv("TARGET", "proposer")
PROPOSER_URLS = [
    #temos dois proposers e o cliente escolhe aleatoriamente entre os dois
    "http://proposer1:9000/propose",
    "http://proposer2:9000/propose"
]

#aqui ficam os resultados que o learner vai mandar
results = {} 
#contador de pedidos do cliente
next_request_id = 1
#cada cliente vai mandar de 10 a 50 pedidos (simular carga real do sistema)
max_requests = random.randint(10, 50)

#esse endpoint é chamado pelo laarner
@app.post("/commit")
def commit():
    data = request.json
    #qual o pedido
    req_id = data.get("request_id") 
    #o resultado
    result = data.get("result")
    # e quem ganhou
    proposal_id = data.get("proposal_id")
    #o clinete salva o resultado
    results[req_id] = {"result": result, "proposal_id": proposal_id}

    print(f"[{CLIENT_ID}] Commit notification for request {req_id}: {result} (proposal {proposal_id})")
    return jsonify({"ok": True})

#essa função envia um pedido ao Proposer
def send_transaction(request_id):

    #escolhe um aleatorio
    PROPOSER_URL = random.choice(PROPOSER_URLS)
    TARGET_NODE = PROPOSER_URL.split('/')[2].split(':')[0]
    
    #montagem de transação 
    payload = {
        "transaction": {
            "client_id": CLIENT_ID,
            "request_id": request_id,
            "timestamp": int(time.time() * 1000),
            "value": f"WRITE_{CLIENT_ID}_{request_id}"
        }
    }
    try:
        #manda o pedido pro Proposer
        r = requests.post(PROPOSER_URL, json=payload, timeout=5)
        print(f"[{CLIENT_ID}] Sent transaction request {request_id} to {TARGET_NODE} (http status {getattr(r,'status_code',None)})")
    except Exception as e:
        print(f"[{CLIENT_ID}] Error sending request {request_id}: {e}")

def main_loop():
    global next_request_id
    numreq = 0

    #cada cliente manda um pedido por vez
    while numreq < max_requests:
        req_id = next_request_id
        next_request_id += 1

        #manda o pedido e fica esperando 15 segundos
        send_transaction(req_id)

        waited = 0
        timeout_ms = 15000 # 15 segundos
        poll_interval = 0.2
        while req_id not in results and waited < timeout_ms/1000.0:
            time.sleep(poll_interval)
            waited += poll_interval

        #caso o pedido seja aceito
        if req_id in results and results[req_id]["result"] == "COMMITTED":
            # Se for COMMIT, espera um pouco e avança para o próximo ID
            numreq += 1
            sleep_time = random.randint(1,5)
            print(f"[{CLIENT_ID}] Request {req_id} COMMITTED. Sleeping {sleep_time}s")
            time.sleep(sleep_time)

        else:
            # Se falhar ou der timeout, tenta novamente 
            print(f"[{CLIENT_ID}] Request {req_id} not committed within timeout. Will retry.")
            time.sleep(random.uniform(1, 5))
        

    print(f"[{CLIENT_ID}] Finished all {max_requests} requests")

def start_background_thread():
    t = threading.Thread(target=main_loop)
    t.daemon = True
    t.start()

if __name__ == "__main__":
    print(f"[{CLIENT_ID}] Starting client; will send {max_requests} transactions to {TARGET_NODE}")
    start_background_thread()
    app.run(host="0.0.0.0", port=5000)
