#!/bin/sh
# Teste ponta a ponta: API -> RabbitMQ -> profissionais / projetos -> RabbitMQ -> notificações.
# Uso: sh scripts/smoke.sh   (com o ambiente no ar)
set -e
API=${API:-http://localhost:3000}

for porta in 3000 3001 3002 3003; do
  printf 'health %s: ' "$porta"
  curl -fsS "http://localhost:$porta/health"; echo
done

echo "--- POST /projetos/recomendacao"
curl -fsS -X POST "$API/projetos/recomendacao" \
  -H 'Content-Type: application/json' \
  -d '{
    "titulo": "Raízes do Sertão",
    "orcamentoTotal": 40000,
    "papeisRequeridos": [
      { "papel": "DIRETOR" }, { "papel": "EDITOR" },
      { "papel": "ROTEIRISTA" }, { "papel": "SONOPLASTA" }
    ]
  }'
echo

sleep 2
echo "--- projetos registrados (porta 3002)"
curl -fsS http://localhost:3002/projetos; echo
echo "--- notificações geradas (porta 3003)"
curl -fsS http://localhost:3003/notificacoes; echo
echo "RabbitMQ: http://localhost:15672 (guest/guest)"
