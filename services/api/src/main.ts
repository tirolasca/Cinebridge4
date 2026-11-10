import Fastify from "fastify";
import { pool, bancoSaudavel } from "./db";
import { conectar, criarCanal, rpcCall, mqSaudavel, FILAS } from "./mq";

const PORTA = Number(process.env.PORT ?? 3000);

interface PapelRequerido {
  papel: string;
  pesoImportancia?: number;
}

interface CorpoRecomendacao {
  titulo?: string;
  orcamentoTotal?: number;
  papeisRequeridos?: PapelRequerido[];
}

interface Candidato {
  id: string;
  nome: string;
  papel: string;
  custo: number;
  notaMedia: number;
}

interface ItemRecomendacao {
  papel: string;
  profissionalId: string | null;
  nome: string | null;
  custo: number;
}

// Normaliza os nomes dos papéis para evitar diferenças
// entre maiúsculas, minúsculas e acentos.
function normalizarPapel(papel: string): string {
  const chave = papel
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();

  const equivalencias: Record<string, string> = {
    DIRETOR: "DIRETOR",
    ROTEIRISTA: "ROTEIRISTA",
    CINEGRAFISTA: "CINEGRAFISTA",
    EDITOR: "EDITOR",
    SONOPLASTA: "SONOPLASTA",
  };

  return equivalencias[chave] ?? chave;
}

// Escolhe o profissional com melhor avaliação.
// Em caso de empate, escolhe o de menor custo.
function escolher(
  papel: string,
  candidatos: Candidato[],
): Candidato | undefined {
  return candidatos
    .filter((c) => normalizarPapel(c.papel) === normalizarPapel(papel))
    .sort(
      (a, b) =>
        Number(b.notaMedia) - Number(a.notaMedia) ||
        Number(a.custo) - Number(b.custo),
    )[0];
}

async function iniciarBanco(): Promise<void> {
  await pool.query(`
    create table if not exists recomendacoes (
      id uuid primary key default gen_random_uuid(),
      projeto_id text,
      titulo text not null,
      resultado jsonb not null,
      criado_em timestamptz not null default now()
    )
  `);
}

async function main(): Promise<void> {
  process.on("SIGTERM", () => process.exit(0));

  await iniciarBanco();

  const conexao = await conectar();
  const canal = await criarCanal(conexao);

  const app = Fastify({ logger: true });

  app.get("/health", async (_req, reply) => {
    const db = await bancoSaudavel();
    const mq = mqSaudavel();

    return reply.code(db && mq ? 200 : 503).send({
      status: db && mq ? "ok" : "degradado",
      db,
      mq,
    });
  });

  app.post<{ Body: CorpoRecomendacao }>(
    "/projetos/recomendacao",
    async (req, reply) => {
      const { titulo, orcamentoTotal, papeisRequeridos } = req.body ?? {};

      // 1. Validar os dados recebidos.
      if (
        !titulo ||
        typeof titulo !== "string" ||
        typeof orcamentoTotal !== "number" ||
        !Number.isFinite(orcamentoTotal) ||
        !Array.isArray(papeisRequeridos)
      ) {
        return reply.code(400).send({
          erro: "titulo, orcamentoTotal e papeisRequeridos são obrigatórios",
        });
      }

      if (orcamentoTotal <= 0 || papeisRequeridos.length === 0) {
        return reply.code(422).send({
          erro: "orçamento deve ser positivo e ao menos um papel é necessário",
        });
      }

      if (
        papeisRequeridos.some(
          (p) => !p || typeof p.papel !== "string" || !p.papel.trim(),
        )
      ) {
        return reply.code(400).send({
          erro: "Todos os papéis precisam possuir um nome válido",
        });
      }

      // 2. Normalizar os papéis.
      const papeis = [
        ...new Set(papeisRequeridos.map((p) => normalizarPapel(p.papel))),
      ];

      // 3. Consultar profissionais pelo RabbitMQ.
      let candidatos: Candidato[];

      try {
        const resp = await rpcCall<{
          candidatos: Candidato[];
        }>(canal, FILAS.consultaProfissionais, { papeis });

        if (!Array.isArray(resp?.candidatos)) {
          throw new Error("Resposta inválida do serviço de profissionais");
        }

        candidatos = resp.candidatos;

        req.log.info(
          {
            papeis,
            quantidadeCandidatos: candidatos.length,
          },
          "Consulta de profissionais concluída",
        );
      } catch (e) {
        req.log.error(e);

        return reply.code(503).send({
          erro: "serviço de profissionais indisponível",
        });
      }

      // 4. Montar a equipe recomendada.
      const itens: ItemRecomendacao[] = papeis.map((papel) => {
        const profissional = escolher(papel, candidatos);

        if (!profissional) {
          req.log.warn(
            { papel },
            "Nenhum profissional encontrado para este papel",
          );

          return {
            papel,
            profissionalId: null,
            nome: null,
            custo: 0,
          };
        }

        return {
          papel,
          profissionalId: profissional.id,
          nome: profissional.nome,
          custo: Number(profissional.custo),
        };
      });

      // 5. Calcular orçamento.
      const parcial = itens.some((item) => item.profissionalId === null);

      const custoPlanejado = itens.reduce((soma, item) => soma + item.custo, 0);

      const dentroDoOrcamento = custoPlanejado <= orcamentoTotal;

      // 6. Registrar projeto pelo RabbitMQ.
      let projetoId: string | null = null;

      try {
        const resp = await rpcCall<{
          projetoId: string;
        }>(canal, FILAS.registroProjetos, {
          titulo,
          orcamentoTotal,
          equipe: itens,
        });

        projetoId = resp.projetoId ?? null;

        if (!projetoId) {
          throw new Error("Serviço de projetos não retornou projetoId");
        }
      } catch (e) {
        req.log.error(e);
      }

      // 7. Montar resultado.
      const resultado = {
        itens,
        parcial,
        custoPlanejado,
        dentroDoOrcamento,
      };

      // 8. Salvar recomendação.
      const { rows } = await pool.query(
        `
          insert into recomendacoes
            (projeto_id, titulo, resultado)
          values ($1, $2, $3)
          returning id
        `,
        [projetoId, titulo, JSON.stringify(resultado)],
      );

      // 9. Retornar resultado.
      return reply.code(201).send({
        recomendacaoId: rows[0].id,
        projetoId,
        ...resultado,
        ...(projetoId
          ? {}
          : {
              aviso: "projeto não registrado: serviço de projetos indisponível",
            }),
      });
    },
  );

  // Consultar recomendações registradas.
  app.get("/recomendacoes", async () => {
    const { rows } = await pool.query(`
      select
        id,
        projeto_id as "projetoId",
        titulo,
        resultado,
        criado_em as "criadoEm"
      from recomendacoes
      order by criado_em desc
      limit 20
    `);

    return rows;
  });

  await app.listen({
    port: PORTA,
    host: "0.0.0.0",
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
