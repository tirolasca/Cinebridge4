import amqp from 'amqplib';
import type { ConsumeMessage } from 'amqplib';
import { randomUUID } from 'node:crypto';

type Conexao = Awaited<ReturnType<typeof amqp.connect>>;
export type Canal = amqp.ConfirmChannel;

export const FILAS = {
  consultaProfissionais: 'consulta-profissionais',
  registroProjetos: 'registro-projetos',
  eventosProjetos: 'eventos-projetos',
} as const;

let saudavel = false;
export const mqSaudavel = (): boolean => saudavel;

const dormir = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Conecta ao RabbitMQ com novas tentativas (o broker pode demorar a aceitar conexões). */
export async function conectar(url = process.env.RABBITMQ_URL ?? '', tentativas = 15): Promise<Conexao> {
  for (let i = 1; ; i++) {
    try {
      const conexao = await amqp.connect(url);
      conexao.on('error', (e: Error) => console.error('[mq] erro de conexão:', e.message));
      conexao.on('close', () => {
        // Sem a conexão o serviço não funciona: encerra e deixa o Docker reiniciar (restart: unless-stopped).
        saudavel = false;
        console.error('[mq] conexão encerrada; finalizando processo');
        process.exit(1);
      });
      saudavel = true;
      console.log('[mq] conectado');
      return conexao;
    } catch (e) {
      if (i >= tentativas) throw e;
      console.warn(`[mq] tentativa ${i}/${tentativas} falhou; nova tentativa em 2s`);
      await dormir(2000);
    }
  }
}

export async function criarCanal(conexao: Conexao): Promise<Canal> {
  const canal = await conexao.createConfirmChannel();
  canal.on('error', (e: Error) => console.error('[mq] erro de canal:', e.message));
  return canal;
}

/** Publica JSON em uma fila durável, aguardando a confirmação do broker. */
export async function publicar(
  canal: Canal,
  fila: string,
  mensagem: unknown,
  opcoes: amqp.Options.Publish = {},
): Promise<void> {
  await canal.assertQueue(fila, { durable: true });
  await new Promise<void>((resolve, reject) => {
    canal.sendToQueue(
      fila,
      Buffer.from(JSON.stringify(mensagem)),
      { persistent: true, contentType: 'application/json', ...opcoes },
      (err) => (err ? reject(err) : resolve()),
    );
  });
}

/** Consome JSON de uma fila durável com confirmação manual (ack após o processamento). */
export async function consumir<T>(
  canal: Canal,
  fila: string,
  tratar: (mensagem: T, bruto: ConsumeMessage) => Promise<void>,
  prefetch = 10,
): Promise<void> {
  await canal.assertQueue(fila, { durable: true });
  await canal.prefetch(prefetch);
  await canal.consume(fila, async (bruto) => {
    if (!bruto) return;
    try {
      await tratar(JSON.parse(bruto.content.toString()) as T, bruto);
      canal.ack(bruto);
    } catch (e) {
      console.error(`[mq] falha ao processar mensagem de ${fila}:`, e);
      canal.nack(bruto, false, false); // descarta (não reenfileira) para evitar laço infinito
    }
  });
  console.log(`[mq] consumindo ${fila}`);
}

/** Responde a uma requisição RPC na fila de retorno indicada pelo remetente. */
export function responder(canal: Canal, bruto: ConsumeMessage, corpo: unknown): void {
  const { replyTo, correlationId } = bruto.properties;
  if (!replyTo) return;
  canal.sendToQueue(replyTo, Buffer.from(JSON.stringify(corpo)), {
    correlationId,
    contentType: 'application/json',
  });
}

/**
 * Requisição/resposta sobre filas: publica em `fila` e aguarda a resposta em uma fila de
 * retorno exclusiva, criada com nome único para cada requisição.
 */
export async function rpcCall<T>(canal: Canal, fila: string, payload: unknown, timeoutMs = 5000): Promise<T> {
  const { queue: replyTo } = await canal.assertQueue('', { exclusive: true, autoDelete: true });
  const correlationId = randomUUID();
  let consumerTag = '';
  let timer: NodeJS.Timeout | undefined;

  const resposta = new Promise<T>((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`RPC em ${fila}: tempo esgotado (${timeoutMs}ms)`)), timeoutMs);
    canal
      .consume(
        replyTo,
        (msg) => {
          if (!msg || msg.properties.correlationId !== correlationId) return;
          const corpo = JSON.parse(msg.content.toString());
          if (corpo && corpo.erro) reject(new Error(String(corpo.erro)));
          else resolve(corpo as T);
        },
        { noAck: true },
      )
      .then((r) => {
        consumerTag = r.consumerTag;
      })
      .catch(reject);
  });

  try {
    await publicar(canal, fila, payload, {
      replyTo,
      correlationId,
      persistent: false,
      expiration: String(timeoutMs), // requisição expirada não deve acumular na fila
    });
    return await resposta;
  } finally {
    if (timer) clearTimeout(timer);
    if (consumerTag) canal.cancel(consumerTag).catch(() => undefined);
  }
}
