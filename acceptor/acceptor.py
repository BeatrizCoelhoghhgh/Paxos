# acceptor.py
#IMPORTAÇÕES
from flask import Flask, request, jsonify
import requests
import os
from prometheus_client import Counter, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

#cria uma aplicação web do acceptor
app = Flask(__name__) 

#Métricas para controle#
#conta quantas mensagens PROMISE foram enviadas
PROMISES_SENT = Counter('paxos_promises_sent_total', 'Total de mensagens PROMISE enviadas') 
#conta quantas mensagens ACCEPT o Acceptor recebeu
ACCEPTS_RECEIVED = Counter('paxos_accepts_received_total', 'Total de mensagens ACCEPT recebidas')
#Conta quantas rejeições (not_promise e not_accepted) foram enviadas
REJECTIONS_SENT = Counter('paxos_rejections_sent_total', 'Total de rejeições enviadas (NoPromise ou NoAccept)')
#Conta quantoas vezes o Acceptor notificou o Learner
NOTIFICATIONS_SENT = Counter('paxos_learner_notifications_total', 'Total de notificações de commit enviadas ao Learner')

#função que lê uma variável de ambiente e trasforma em lista
def load_urls_from_env(env_var_name, default_value=""):
    """Lê uma string separada por vírgulas de uma variável de ambiente e a converte em uma lista de URLs."""
    urls_str = os.getenv(env_var_name, default_value) #Pega a variável do sistema
    return [url.strip() for url in urls_str.split(',') if url.strip()] #Separa as URLs por vírgula e remove espaços

# Carrega a lista de Learners
#Define para onde o Acceptor vai mandar o resultado do voto.
LEARNERS = load_urls_from_env(
    "LEARNER_URLS",
    "http://learner1:8200/learn,http://learner2:8200/learn"
)

# --- ESTADO DO ACCEPTOR 

#maior prefixo prometido
highest_promised_prefix = 0 # A parte numérica mais alta do TID para a qual uma promessa já foi feita. Usado para rejeitar PREPAREs antigos.
#ID completo da maior promessa
highest_promised_id = None #O TID completo mais alto para o qual uma promessa foi feita.

#ID aceito 
accepted_id = None #O TID do valor que este Acceptor aceitou por último.
#valor aceito
accepted_value = None #O valor (transação) que este Acceptor aceitou por último.
#Identifocador unico do Acceptor
ACCEPTOR_ID = os.getenv("HOSTNAME", "acceptor")

#extrai o numero do proposal_id (prioridade maior)
def prefix_from_pid(pid):
    """Extrai o prefixo numérico do proposal_id (o TID) para comparação."""
    try:
        # O TID tem o formato <prefixo_numérico>:<proposer_id>
        return int(str(pid).split(":")[0])
    except:
        return 0

#Recebe uma mensagem PREPARE do Proposer
# FASE 1: PREPARE/PROMISE 
@app.post("/prepare")
def prepare():
    #O Acceptor vai ler e atualizar seu estado interno
    global highest_promised_prefix, highest_promised_id, accepted_id, accepted_value
    #Extrai o ID da proposta.
    data = request.get_json()
    proposal_id = data.get("proposal_id")
    print(f"[ACCEPTOR] Received PREPARE: {data}", flush=True)
    #Extrai o número da proposta.
    req_prefix = prefix_from_pid(proposal_id)
    
    # Se a proposta for maior ou igual ao maior prefixo prometido
    if req_prefix >= highest_promised_prefix:
        highest_promised_prefix = req_prefix
        highest_promised_id = proposal_id
        
        PROMISES_SENT.inc()
        # Resposta "promise" (promessa)
        response = {
            "type": "promise",
            "tid_in_use": highest_promised_id,  #maior ID prometido
            "accepted_id": accepted_id,
            "accepted_value": accepted_value #se já aceitou algo antes
        }

        #se não a proposta é rejeitada por ser antiga!
        return jsonify(response), 200 
    else:
        REJECTIONS_SENT.inc()
        # Resposta "not_promise" (conflito)
        response = {
            "type": "not_promise",
            "tid_in_use": highest_promised_id, #pra recalcular e tentar de novo 
            "accepted_id": accepted_id,
            "accepted_value": accepted_value
        }
        return jsonify(response), 409

# FUNÇÃO DE NOTIFICAÇÃO 
#envia o voto do acceptor para os Learners
def notify_learners(proposal_id, transaction, accepted_status):
    notify_payload = {
        "acceptor_id": ACCEPTOR_ID,
        "proposal_id": proposal_id,
        "accepted": accepted_status, #true ou false
        "transaction": transaction
    }
    
    for url in LEARNERS:
        try:
            requests.post(url, json=notify_payload, timeout=0.5)
        except Exception:
            # Ignora falhas de comunicação com Learners
            pass

# FASE 2: ACCEPT/ACCEPTED 
#recebe pedido para aceitar o voto
@app.post("/accept")
def accept():
    global accepted_id, accepted_value, highest_promised_id, highest_promised_prefix
    data = request.get_json()
    print(f"[ACCEPTOR] Received ACCEPT: {data}", flush=True)
    #Proposta + valor a ser decidido.
    proposal_id = data.get("proposal_id")
    transaction = data.get("transaction")

    req_prefix = prefix_from_pid(proposal_id)
    
    # Regra de Aceitação: A proposta deve ter um prefixo maior ou igual ao maior prometido
    if req_prefix >= highest_promised_prefix:
        # 1. Atualiza o estado para o valor aceito
        accepted_id = proposal_id
        accepted_value = transaction
        
        # 2. Atualiza a promessa mais alta (garante que propostas antigas sejam rejeitadas no futuro)
        #rejeita propostas antigas
        highest_promised_prefix = req_prefix
        highest_promised_id = proposal_id

        # 3. Notifica Learners e Proposer do aceite 
        notify_learners(proposal_id, transaction, accepted_status=True)

        accepted_id = None
        accepted_value = None

        ACCEPTS_RECEIVED.inc()
        NOTIFICATIONS_SENT.inc()
        
        return jsonify({"response": "accepted", "tid": proposal_id}), 200
    else:
        # Notifica Learners e Proposer da rejeição 
        #avisa o Learner: votei não 
        notify_learners(proposal_id, transaction, accepted_status=False)
        
        REJECTIONS_SENT.inc()
        # Envia o TID em uso para ajudar o Proposer a se corrigir
  
        return jsonify({"response": "not_accepted", "tid": proposal_id, "tid_in_use": highest_promised_id}), 409

@app.get("/")
def root():
    return "ACCEPTOR OK"

app_dispatcher = DispatcherMiddleware(app, {
    '/metrics': make_wsgi_app()
})

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    
    # O Acceptor deve rodar na porta 8000 para a rede interna do Paxos
    print("ACCEPTOR starting on port 8000. Paxos endpoints and /metrics exposed.")
    
    #SERVIDOR
    # Executa o DispatcherMiddleware na porta 8000
    run_simple('0.0.0.0', 8000, app_dispatcher)
