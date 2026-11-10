import Fastify from 'fastify';
import { pool, bancoSaudavel } from './db';
import { conectar, criarCanal, consumir, responder, mqSaudavel, FILAS } from './mq';

const PORTA = Number(process.env.PORT ?? 3001);

const COLUNAS = `id, nome, papel, custo::float8 as custo, nota_media::float8 as "notaMedia", competencias`;

async function iniciarBanco(): Promise<void> {
  await pool.query(`
    create table if not exists profissionais (
      id uuid primary key default gen_random_uuid(),
      nome text not null,
      papel text not null,
      custo numeric(12,2) not null,
      nota_media numeric(3,2) not null default 0,
      competencias text[] not null default '{}'
    )`);
  const { rows } = await pool.query('select count(*)::int as n from profissionais');
  if (rows[0].n === 0) {
    await pool.query(`
      insert into profissionais (nome, papel, custo, nota_media, competencias) values
      ('Marina Alves',   'DIRETOR',    12000, 4.8, '{direcao,documentario}'),
      ('Rafael Costa',   'DIRETOR',     9000, 4.2, '{direcao,publicidade}'),
      ('Camila Ferreira','EDITOR',      9000, 4.7, '{edicao,color}'),
      ('Bruno Lima',     'EDITOR',      6500, 4.0, '{edicao}'),
      ('Helena Souza',   'ROTEIRISTA',  6000, 4.6, '{roteiro,documentario}'),
      ('Diego Martins',  'ROTEIRISTA',  4500, 3.9, '{roteiro}'),
      ('Paula Nunes',    'SONOPLASTA',  5000, 4.5, '{mixagem,foley}'),
      ('Igor Barros',    'SONOPLASTA',  3500, 4.1, '{mixagem}')`);
    console.log('[db] base de exemplo carregada');
  }
}

async function main(): Promise<void> {
  process.on('SIGTERM', () => process.exit(0));
  await iniciarBanco();

  const conexao = await conectar();
  const canal = await criarCanal(conexao);

  // Consumidor: responde às consultas de profissionais feitas pela API via RabbitMQ.
  await consumir<{ papeis: string[] }>(canal, FILAS.consultaProfissionais, async (msg, bruto) => {
    try {
      const { rows } = await pool.query(
        `select ${COLUNAS} from profissionais where papel = any($1::text[])`,
        [msg.papeis ?? []],
      );
      responder(canal, bruto, { candidatos: rows });
    } catch (e) {
      console.error('[consulta] erro:', e);
      responder(canal, bruto, { erro: 'falha ao consultar profissionais' });
    }
  });

  const app = Fastify({ logger: true });

  app.get('/health', async (_req, reply) => {
    const db = await bancoSaudavel();
    const mq = mqSaudavel();
    return reply.code(db && mq ? 200 : 503).send({ status: db && mq ? 'ok' : 'degradado', db, mq });
  });

  app.get<{ Querystring: { papel?: string } }>('/profissionais', async (req) => {
    const { rows } = req.query.papel
      ? await pool.query(`select ${COLUNAS} from profissionais where papel = $1 order by nome`, [req.query.papel])
      : await pool.query(`select ${COLUNAS} from profissionais order by nome`);
    return rows;
  });

  app.post<{ Body: { nome?: string; papel?: string; custo?: number; notaMedia?: number; competencias?: string[] } }>(
    '/profissionais',
    async (req, reply) => {
      const { nome, papel, custo, notaMedia = 0, competencias = [] } = req.body ?? {};
      if (!nome || !papel || typeof custo !== 'number') {
        return reply.code(400).send({ erro: 'nome, papel e custo são obrigatórios' });
      }
      const { rows } = await pool.query(
        `insert into profissionais (nome, papel, custo, nota_media, competencias)
         values ($1,$2,$3,$4,$5) returning ${COLUNAS}`,
        [nome, papel, custo, notaMedia, competencias],
      );
      return reply.code(201).send(rows[0]);
    },
  );

  await app.listen({ port: PORTA, host: '0.0.0.0' });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
