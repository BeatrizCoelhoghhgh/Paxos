
FROM python:3.11-slim

# Diretório principal
WORKDIR /app

# Copia o código do proposer
COPY proposer.py .

RUN pip install flask requests prometheus_client
# Instala as dependências reais usadas no código
RUN pip install --no-cache-dir flask requests

# Porta interna usada pelo proposer
EXPOSE 8100

# Comando de execução
CMD ["python", "-u", "proposer.py"]
