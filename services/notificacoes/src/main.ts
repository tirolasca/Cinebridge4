import Fastify from 'fastify';
import { pool, bancoSaudavel } from './db';
import { conectar, criarCanal, consumir, mqSaudavel, FILAS } from './mq';

const PORTA = Number(process.env.PORT ?? 3003);

type TipoEvento = 'PROJETO_CRIADO' | 'PROJETO_ATUALIZADO' | 'PROJETO_FINALIZADO';

interface EventoProjeto {
  tipo: TipoEvento;
  projetoId: string;
  titulo: string;
  tamanhoEquipe: number;
  ocorridoEm: string;
}

// ---- Padrão Observer: cada observador reage a um tipo de evento, sem conhecer os demais ----
interface Observador {
  tratar(evento: EventoProjeto): Promise<void>;
}

class BarramentoEventos {
  private readonly observadores = new Map<TipoEvento, Observador[]>();

  inscrever(tipo: TipoEvento, observador: Observador): void {
    this.observadores.set(tipo, [...(this.observadores.get(tipo) ?? []), observador]);
  }

  async publicar(evento: EventoProjeto): Promise<void> {
    for (const o of this.observadores.get(evento.tipo) ?? []) await o.tratar(evento);
  }
}

class EnvioNotificacao implements Observador {
  constructor(private readonly montar: (e: EventoProjeto) => { assunto: string; mensagem: string }) {}

  async tratar(evento: EventoProjeto): Promise<void> {
    const { assunto, mensagem } = this.montar(evento);
    await pool.query(
      'insert into notificacoes (tipo, assunto, mensagem, evento) values ($1,$2,$3,$4)',
      [evento.tipo, assunto, mensagem, JSON.stringify(evento)],
    );
    // Envio simulado: em produção, trocar pelo provedor de e-mail/mensageria interna.
    console.log(`[notificação] ${assunto} — ${mensagem}`);
  }
}

const barramento = new BarramentoEventos();
barramento.inscrever(
  'PROJETO_CRIADO',
  new EnvioNotificacao((e) => ({
    assunto: `Projeto criado: ${e.titulo}`,
    mensagem: `Equipe recomendada com ${e.tamanhoEquipe} profissional(is) para o projeto ${e.projetoId}.`,
  })),
);
barramento.inscrever(
  'PROJETO_ATUALIZADO',
  new EnvioNotificacao((e) => ({
    assunto: `Projeto atualizado: ${e.titulo}`,
    mensagem: `O projeto ${e.projetoId} foi atualizado.`,
  })),
);
barramento.inscrever(
  'PROJETO_FINALIZADO',
  new EnvioNotificacao((e) => ({
    assunto: `Projeto finalizado: ${e.titulo}`,
    mensagem: `O projeto ${e.projetoId} foi finalizado.`,
  })),
);

async function iniciarBanco(): Promise<void> {
  await pool.query(`
    create table if not exists notificacoes (
      id uuid primary key default gen_random_uuid(),
      tipo text not null,
      assunto text not null,
      mensagem text not null,
      evento jsonb not null,
      criado_em timestamptz not null default now()
    )`);
}

async function main(): Promise<void> {
  process.on('SIGTERM', () => process.exit(0));
  await iniciarBanco();

  const conexao = await conectar();
  const canal = await criarCanal(conexao);
  await consumir<EventoProjeto>(canal, FILAS.eventosProjetos, (evento) => barramento.publicar(evento));

  const app = Fastify({ logger: true });

  app.get('/health', async (_req, reply) => {
    const db = await bancoSaudavel();
    const mq = mqSaudavel();
    return reply.code(db && mq ? 200 : 503).send({ status: db && mq ? 'ok' : 'degradado', db, mq });
  });

  app.get('/notificacoes', async () => {
    const { rows } = await pool.query(
      'select id, tipo, assunto, mensagem, criado_em as "criadoEm" from notificacoes order by criado_em desc limit 50',
    );
    return rows;
  });

  await app.listen({ port: PORTA, host: '0.0.0.0' });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
