# Cinebridge — ATVIV (containerização)

Ambiente completo conforme o diagrama de implantação da atividade: 4 microsserviços Node.js/TypeScript,
4 bancos PostgreSQL (um por serviço), RabbitMQ e a rede `cinebridge-net`.

| Container | Porta (host) | Papel |
|---|---|---|
| `cinebridge-api` | 3000 | Recomendação / ponto de entrada REST |
| `cinebridge-profissionais` | 3001 | Cadastro e consulta de profissionais |
| `cinebridge-projetos` | 3002 | Ciclo de vida dos projetos |
| `cinebridge-notificacoes` | 3003 | Envio de notificações (consome eventos) |
| `cinebridge-mq` | 5672 / 15672 | RabbitMQ (AMQP / painel de gerenciamento) |
| `profissionais-db`, `projetos-db`, `recomendacoes-db`, `notificacoes-db` | 5432–5435 | PostgreSQL 18 |

## Como subir

Pré-requisito: Docker Desktop (Windows 10+) ou Docker Engine + Compose (Ubuntu 24.04+).

```
docker compose up --build -d
docker compose ps          # aguarde todos "healthy"
docker compose logs -f
sh scripts/smoke.sh        # teste ponta a ponta (no Windows: Git Bash/WSL, ou use os curl abaixo)
docker compose down        # mantém os dados  |  docker compose down -v  descarta os volumes
```

Para trocar senhas/usuários: `cp .env.example .env` e edite. Painel do RabbitMQ: http://localhost:15672 (guest/guest).

## Fluxo de mensagens

```
POST /projetos/recomendacao (api)
  ├─ RPC  → fila consulta-profissionais → profissionais responde na fila de retorno (nome único por requisição)
  ├─ RPC  → fila registro-projetos      → projetos grava, responde com o id e publica evento
  └─ projetos → fila eventos-projetos   → notificacoes consome (Observer por tipo de evento) e persiste
```

Filas duráveis, mensagens persistentes, publicação com confirmação do broker (`ConfirmChannel`) e
consumo com `ack` manual. Todos os serviços expõem `GET /health` (verifica banco e RabbitMQ; 503 se algum falhar).

## Teste manual

```
curl -X POST http://localhost:3000/projetos/recomendacao -H "Content-Type: application/json" -d '{
  "titulo": "Raízes do Sertão", "orcamentoTotal": 40000,
  "papeisRequeridos": [{"papel":"DIRETOR"},{"papel":"EDITOR"},{"papel":"ROTEIRISTA"},{"papel":"SONOPLASTA"}]
}'
curl http://localhost:3002/projetos
curl http://localhost:3003/notificacoes
```

## Integração com o seu repositório

Esta pasta é um esqueleto de referência da arquitetura distribuída: o critério de recomendação em
`services/api/src/main.ts` é simplificado (melhor nota, desempate por custo). Para o trabalho final:

1. Mova o domínio do Cinebridge3 (estratégias, tipos de projeto, decoradores, adaptadores) para `services/api`.
2. Mantenha `src/mq.ts` e `src/db.ts` como estão; a API só precisa chamar `rpcCall` no lugar do adaptador HTTP/interno de profissionais.
3. Substitua o seed de `profissionais` pela sua base de exemplo.

## Versões

| Componente | Versão |
|---|---|
| Node.js (imagem `node:26-alpine`) | 26 LTS |
| PostgreSQL (`postgres:18-alpine`) | 18 |
| RabbitMQ (`rabbitmq:4-management-alpine`) | 4.x |
| `amqplib` / `fastify` / `pg` | 2.3 / 5.12 / 8.23 |
| `typescript` / `@types/node` / `@types/pg` | 7.0 / 26 / 8.23 |

Os Dockerfiles usam `npm ci` com os `package-lock.json` versionados, para builds reproduzíveis.

**Atenção ao atualizar um ambiente que já existia:** o PostgreSQL 18 grava os dados em outro caminho dentro do
volume (por isso o compose monta `/var/lib/postgresql`) e não lê dados de uma versão anterior. Em ambientes antigos,
rode `docker compose down -v` antes do primeiro `up`.

Para atualizar depois: `npm outdated` em cada serviço, `npm update` e rode `sh scripts/smoke.sh`.

## Limitações conhecidas

- Mensagens que falham no consumo são descartadas (`nack` sem reenfileirar); em produção, usar uma dead-letter queue.
- O serviço de projetos grava no banco e depois publica o evento (sem *outbox*); uma queda entre os dois perde o evento.

