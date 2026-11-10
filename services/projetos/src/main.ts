import Fastify from 'fastify';
import { pool, bancoSaudavel } from './db';
import { conectar, criarCanal, consumir, publicar, responder, mqSaudavel, Canal, FILAS } from './mq';

const PORTA = Number(process.env.PORT ?? 3002);

const COLUNAS = `id, titulo, orcamento_total::float8 as "orcamentoTotal", status, equipe, criado_em as "criadoEm"`;

interface NovoProjeto {
  titulo: string;
  orcamentoTotal: number;
  equipe?: unknown[];
}

async function iniciarBanco(): Promise<void> {
  await pool.query(`
    create table if not exists projetos (
      id uuid primary key default gen_random_uuid(),
      titulo text not null,
      orcamento_total numeric(14,2) not null,
      status text not null default 'CRIADO',
      equipe jsonb not null default '[]',
      criado_em timestamptz not null default now()
    )`);
}

async function main(): Promise<void> {
  process.on('SIGTERM', () => process.exit(0));
  await iniciarBanco();

  const conexao = await conectar();
  const canalPub: Canal = await criarCanal(conexao);
  const canalSub: Canal = await criarCanal(conexao);

  async function emitirEvento(tipo: string, projeto: { id: string; titulo: string; equipe?: unknown[] }) {
    await publicar(canalPub, FILAS.eventosProjetos, {
      tipo,
      projetoId: projeto.id,
      titulo: projeto.titulo,
      tamanhoEquipe: projeto.equipe?.length ?? 0,
      ocorridoEm: new Date().toISOString(),
    });
  }

  async function criarProjeto(p: NovoProjeto) {
    const { rows } = await pool.query(
      `insert into projetos (titulo, orcamento_total, equipe) values ($1,$2,$3) returning ${COLUNAS}`,
      [p.titulo, p.orcamentoTotal, JSON.stringify(p.equipe ?? [])],
    );
    await emitirEvento('PROJETO_CRIADO', rows[0]);
    return rows[0];
  }

  // Consumidor: a API registra o projeto recomendado por mensagem (sem chamada HTTP direta).
  await consumir<NovoProjeto>(canalSub, FILAS.registroProjetos, async (msg, bruto) => {
    try {
      const projeto = await criarProjeto(msg);
      responder(canalSub, bruto, { projetoId: projeto.id });
    } catch (e) {
      console.error('[registro] erro:', e);
      responder(canalSub, bruto, { erro: 'falha ao registrar projeto' });
    }
  });

  const app = Fastify({ logger: true });

  app.get('/health', async (_req, reply) => {
    const db = await bancoSaudavel();
    const mq = mqSaudavel();
    return reply.code(db && mq ? 200 : 503).send({ status: db && mq ? 'ok' : 'degradado', db, mq });
  });

  app.get('/projetos', async () => (await pool.query(`select ${COLUNAS} from projetos order by criado_em desc`)).rows);

  app.get<{ Params: { id: string } }>('/projetos/:id', async (req, reply) => {
    const { rows } = await pool.query(`select ${COLUNAS} from projetos where id = $1`, [req.params.id]);
    return rows[0] ?? reply.code(404).send({ erro: 'projeto não encontrado' });
  });

  app.post<{ Body: Partial<NovoProjeto> }>('/projetos', async (req, reply) => {
    const { titulo, orcamentoTotal, equipe } = req.body ?? {};
    if (!titulo || typeof orcamentoTotal !== 'number') {
      return reply.code(400).send({ erro: 'titulo e orcamentoTotal são obrigatórios' });
    }
    return reply.code(201).send(await criarProjeto({ titulo, orcamentoTotal, equipe }));
  });

  app.patch<{ Params: { id: string }; Body: { titulo?: string; status?: 'EM_ANDAMENTO' | 'FINALIZADO' } }>(
    '/projetos/:id',
    async (req, reply) => {
      const { titulo, status } = req.body ?? {};
      if (status && !['EM_ANDAMENTO', 'FINALIZADO'].includes(status)) {
        return reply.code(400).send({ erro: 'status deve ser EM_ANDAMENTO ou FINALIZADO' });
      }
      const { rows } = await pool.query(
        `update projetos set titulo = coalesce($2, titulo), status = coalesce($3, status)
         where id = $1 returning ${COLUNAS}`,
        [req.params.id, titulo ?? null, status ?? null],
      );
      if (!rows[0]) return reply.code(404).send({ erro: 'projeto não encontrado' });
      await emitirEvento(status === 'FINALIZADO' ? 'PROJETO_FINALIZADO' : 'PROJETO_ATUALIZADO', rows[0]);
      return rows[0];
    },
  );

  await app.listen({ port: PORTA, host: '0.0.0.0' });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
