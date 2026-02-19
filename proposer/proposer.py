# proposer.py

import requests
from flask import Flask, request, jsonify
import threading # Novo import para assincronismo
import time
import os
import random
from prometheus_client import Counter, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

app = Flask(__name__)

# --- CONFIGURAÇÃO DE REDE LIDA DE VARIÁVEIS DE AMBIENTE ---

def load_urls_from_env(env_var_name, default_value=""):
    """Lê uma string separada por vírgulas de uma variável de ambiente e a converte em uma lista de URLs."""
    urls_str = os.getenv(env_var_name, default_value)
    return [url.strip() for url in urls_str.split(',') if url.strip()]

# Carrega as listas de Acceptors e Learners
ACCEPTORS = load_urls_from_env(
    "ACCEPTOR_URLS",
    "http://acceptor1:8000,http://acceptor2:8000,http://acceptor3:8000"
)

LEARNERS = load_urls_from_env(
    "LEARNER_URLS",
    "http://learner1:8200/learn,http://learner2:8200/learn"
)

# --- CONFIGURAÇÃO DE PAXOS ---
PROPOSER_ID = os.getenv("HOSTNAME", "proposer") #id unico do proposer
local_counter = int(time.time() * 1000) # Contador global para IDs

if len(ACCEPTORS) > 0:
    MAJORITY = (len(ACCEPTORS) // 2) + 1 #calcula o quorum
else:
    MAJORITY = 2 

# Configuração para Retry/Backoff
BASE_BACKOFF_MIN = float(os.getenv("PROPOSER_BASE_BACKOFF_MIN", "1.0")) # Mínimo de 1.0 segundos
BASE_BACKOFF_MAX = float(os.getenv("PROPOSER_BASE_BACKOFF_MAX", "5.0")) # Máximo de 5.0 segundos
MAX_BACKOFF = float(os.getenv("PROPOSER_MAX_BACKOFF", "5.0")) # Mantido em 5.0 para este intervalo

# --- MÉTRICAS PROMETHEUS ---
PAXOS_ATTEMPTS = Counter('paxos_attempts_total', 'Total de requisições /propose do cliente')
PREPARES_SENT = Counter('paxos_prepares_sent_total', 'Total de mensagens PREPARE enviadas')
PROMISES_QUORUM_FAIL = Counter('paxos_promises_quorum_fail_total', 'Total de falhas de Quorum na Fase 1 (Prepare)')
ACCEPTS_QUORUM_FAIL = Counter('paxos_accepts_quorum_fail_total', 'Total de falhas de Quorum na Fase 2 (Accept)')
COMMITS_TOTAL = Counter('paxos_commits_total', 'Total de propostas concluídas com sucesso (COMMITTED)')

# FUNÇÕES DE ID

def make_proposal_id():
    global local_counter
    local_counter += 1
    # Formato TID: <prefixo_numérico>:<proposer_id>
    return f"{local_counter}:{PROPOSER_ID}"

def prefix_from_pid(pid):
    """Extrai o prefixo numérico do proposal_id (o TID) para comparação."""
    try:
        return int(str(pid).split(":")[0])
    except:
        return 0

#resolve tudo quanto é B.O
def bump_proposal_id_based_on_feedback(original_id, responses):
    global local_counter 
    
    highest = 0 # Usar 0 como base
    
    # Encontra o maior TID visto em qualquer resposta
    for r in responses:
        # Verifica tid_in_use (da fase Prepare/NotPromise) e tid (da fase Accept/NotAccepted)
        tid = r.get("tid_in_use") or r.get("tid") or r.get("accepted_id")
        if tid:
            prefix = prefix_from_pid(tid)
            if prefix > highest:
                highest = prefix

    # Novo prefixo é um a mais que o maior visto
    new_prefix = highest + 1
    
    # Atualiza o contador global para evitar conflito com novas transações
    if new_prefix > local_counter:
        local_counter = new_prefix
    
    # Preserva o ID do proposer original no sufixo
    proposer_id_suffix = str(original_id).split(":")[1] if ":" in str(original_id) else PROPOSER_ID
    return f"{local_counter}:{proposer_id_suffix}" # Retorna o ID baseado no contador global atualizado

# FUNÇÕES DE COMUNICAÇÃO

def send_prepare_to_all(proposal_id, transaction, timeout=3):
    prepare_payload = {"proposal_id": proposal_id, "transaction": transaction}
    promises = []
    not_promises = []
    print(f"[PROPOSER] Sending PREPARE {proposal_id} with payload: {prepare_payload}", flush=True)
    #pra cada um dos acceptors envia um PREPARE
    for acc_url in ACCEPTORS:
        try:
            r = requests.post(f"{acc_url}/prepare", json=prepare_payload, timeout=timeout)
            body = r.json()
            print(f"[PROPOSER] Received response from {acc_url} (PREPARE): Status {r.status_code}, Body: {body}", flush=True)
            #conta os promisses e not-promisses
            if r.status_code == 200 and body and body.get("type") == "promise":
                promises.append(body)
            else:
                not_promises.append(body or {"type": "not_promise", "tid_in_use": None})
        except Exception:
            not_promises.append({"type": "not_promise", "tid_in_use": None})
    return promises, not_promises

def send_accept_to_all(proposal_id, transaction, timeout=3):
    accept_payload = {"proposal_id": proposal_id, "transaction": transaction}
    accepts = []
    not_accepts = []
    print(f"[PROPOSER] Sending ACCEPT {proposal_id} with transaction: {transaction}", flush=True)
    #pra cada um dos acceptors envia um ACCEPT!
    for acc_url in ACCEPTORS:
        try:
            r = requests.post(f"{acc_url}/accept", json=accept_payload, timeout=timeout)
            body = r.json()
            print(f"[PROPOSER] Received response from {acc_url} (ACCEPT): Status {r.status_code}, Body: {body}", flush=True)
            #conta os acceps e not-accepts
            if r.status_code == 200 and body and body.get("response") == "accepted":
                accepts.append(body)
            else:
                not_accepts.append(body or {"response": "not_accepted", "tid": proposal_id})
        except Exception:
            not_accepts.append({"response": "not_accepted", "tid": proposal_id})
    return accepts, not_accepts

# PAXOS

def run_paxos(proposal_id, transaction):
    """Contém o loop de consenso Paxos, rodando em uma thread separada."""
    current_proposal_id = proposal_id
    
    # Loop Infinito - Garante que o retry continue até o sucesso
    while True:
        # FASE 1: PREPARE
        PREPARES_SENT.inc()
        promises, not_promises = send_prepare_to_all(current_proposal_id, transaction)

        if len(promises) < MAJORITY:
            # Falha na maioria da Fase 1: Recalcula novo ID, aplica backoff e repete
            PROMISES_QUORUM_FAIL.inc() 
            all_responses = promises + not_promises
            current_proposal_id = bump_proposal_id_based_on_feedback(current_proposal_id, all_responses)
            
            # Backoff (não bloqueia o servidor, apenas a thread de Paxos)
            sleep_time = random.uniform(BASE_BACKOFF_MIN, BASE_BACKOFF_MAX)
            print(f"[PROPOSER] Quorum failure (Phase 1). Backing off for {sleep_time:.2f}s.", flush=True)
            time.sleep(sleep_time)
            continue 

        original_transaction = transaction.copy()
        # Trata a regra de adoção de valor do Paxos (se já houve um valor aceito)
        highest = None
        for p in promises:
            accepted_id = p.get("accepted_id")
            accepted_value = p.get("accepted_value")
            
            if accepted_id and accepted_value:
                prefix = prefix_from_pid(accepted_id)
                if highest is None or prefix > highest["prefix"]:
                    highest = {"prefix": prefix, "accepted_id": accepted_id, "accepted_value": accepted_value}

        if highest:
            if transaction != highest["accepted_value"]:
             print(f"[PROPOSER] WARNING: Adopted older value (Request ID: {transaction.get('request_id', 'N/A')}). Delaying Phase 2 by {BASE_BACKOFF}s.", flush=True)
             time.sleep(BASE_BACKOFF)

        # FASE 2: ACCEPT
        accepts, not_accepts = send_accept_to_all(current_proposal_id, transaction)

        if len(accepts) >= MAJORITY:
            # Sucesso: A maioria aceitou o valor
            COMMITS_TOTAL.inc()

            return # Sai da função, terminando a thread
        else:
            # Falha na maioria da Fase 2: Recalcula novo ID, aplica backoff e repete
            ACCEPTS_QUORUM_FAIL.inc()
            all_responses = accepts + not_accepts
            current_proposal_id = bump_proposal_id_based_on_feedback(current_proposal_id, all_responses)
            
            sleep_time = random.uniform(BASE_BACKOFF_MIN, BASE_BACKOFF_MAX)
            print(f"[PROPOSER] Quorum failure (Phase 2). Backing off for {sleep_time:.2f}s.", flush=True)
            time.sleep(sleep_time)
            continue 


# --- ENDPOINT PRINCIPAL (Não-Bloqueante) ---

@app.post("/propose")
def propose():
    data = request.get_json()
    transaction = data.get("transaction")
    if not transaction:
        return jsonify({"error": "missing transaction"}), 400

    PAXOS_ATTEMPTS.inc() # Incrementa o contador de requisições do cliente

    # 1. Gera o primeiro ID e encapsula os dados
    proposal_id = make_proposal_id()

    # 2. Inicia o Paxos em uma thread separada
    thread = threading.Thread(target=run_paxos, args=(proposal_id, transaction))
    thread.start()

    # 3. Retorna imediatamente (Não-Bloqueante)
    return jsonify({"status": "PENDING", "proposal_id": proposal_id}), 202

# --- CONFIGURAÇÃO DO SERVIDOR ---

# Encapsula a aplicação Flask com o endpoint /metrics do Prometheus
app_dispatcher = DispatcherMiddleware(app, {
    '/metrics': make_wsgi_app()
})

@app.get("/")
def root():
    return "PROPOSER OK"

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print("PROPOSER starting on port 9000 with /metrics exposed.")
    run_simple('0.0.0.0', 9000, app_dispatcher)
