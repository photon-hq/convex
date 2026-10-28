import {
  ApiError,
  Photon,
  ResponseValidationError,
  TransportError,
} from "@photon-ai/api";
import type { FunctionReturnType } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { internalAction } from "./_generated/server.js";

/**
 * Carries out one outbox row against the Photon API.
 *
 * Runs in Convex's default runtime: `@photon-ai/api` speaks HTTP through
 * `fetch`, so no `"use node"` action is needed.
 */

type Job = NonNullable<FunctionReturnType<typeof internal.outbox.loadJob>>;

/** What `outbox.settle` records for a row. */
interface Outcome {
  error?: string;
  messageId?: string;
  ok: boolean;
  retryable?: boolean;
}

const ACCEPTED_STATUS = 202;
const RETRYABLE_STATUSES: ReadonlySet<number> = new Set([408, 429]);
const MIN_SERVER_ERROR_STATUS = 500;

/** Not sent, and retrying cannot help. */
const fail = (error: string): Outcome => ({
  ok: false,
  error,
  retryable: false,
});

/**
 * Settle a returned Message. A Message the client does not recognise comes
 * back wrapped as `{ platform: "UNKNOWN", raw }`, with its fields under `raw`.
 */
const settleMessage = (message: unknown): Outcome => {
  interface Fields {
    failure?: { code?: unknown };
    id?: unknown;
    status?: unknown;
  }
  const fields = message as Fields & { platform?: unknown; raw?: Fields };
  const source = fields.platform === "UNKNOWN" ? (fields.raw ?? {}) : fields;
  if (source.status === "failed") {
    const code = source.failure?.code;
    return fail(
      `message status is failed${typeof code === "string" ? `: ${code}` : ""}`
    );
  }
  return {
    ok: true,
    ...(typeof source.id === "string" ? { messageId: source.id } : {}),
  };
};

const problemCode = (body: unknown): string | undefined => {
  const code = (body as { code?: unknown } | null)?.code;
  return typeof code === "string" ? code : undefined;
};

/** Settle a thrown error: retry only what another attempt can fix. */
const settleError = (error: unknown): Outcome => {
  if (
    error instanceof ResponseValidationError &&
    error.status === ACCEPTED_STATUS
  ) {
    // The API accepted the request; only the response did not match the
    // schema this client was generated from.
    return { ok: true };
  }
  if (error instanceof ApiError) {
    const code = problemCode(error.body);
    const summary = `${error.status}${code ? ` ${code}` : ""}: ${error.message}`;
    return {
      ok: false,
      error: summary,
      retryable:
        RETRYABLE_STATUSES.has(error.status) ||
        error.status >= MIN_SERVER_ERROR_STATUS,
    };
  }
  if (error instanceof TransportError) {
    return { ok: false, error: error.message, retryable: true };
  }
  return fail(error instanceof Error ? error.message : String(error));
};

const run = async (job: Job): Promise<Outcome> => {
  if (!job.config) {
    return fail("no Photon API credentials: pass projectId and apiKey");
  }
  // Created per run, so the client picks up the current `fetch`. Retries are
  // the outbox's job, so the client's own are off.
  const photon = new Photon({
    ...(job.config.baseUrl === undefined
      ? {}
      : { baseUrl: job.config.baseUrl }),
    headers: { Authorization: `Bearer ${job.config.apiKey}` },
    retry: false,
  });
  const id = job.config.projectId;
  try {
    if (job.kind === "markMessageRead") {
      if (!job.targetMessageId) {
        return fail("no message to mark read: pass a turnId");
      }
      return settleMessage(
        await photon.projects.markMessageRead({
          path: { id, messageId: job.targetMessageId },
          headers: { "idempotency-key": job.idempotencyKey },
        })
      );
    }
    // Sending and typing both act as the agent's own user in the space.
    if (!job.senderId) {
      return fail("no senderId for this space: no event with receivedBy yet");
    }
    if (job.kind === "setSpaceTyping") {
      return settleMessage(
        await photon.projects.setSpaceTyping({
          path: { id, spaceId: job.spaceId },
          body: { senderId: job.senderId, typing: true },
        })
      );
    }
    return settleMessage(
      await photon.projects.sendMessage({
        path: { id },
        headers: { "idempotency-key": job.idempotencyKey },
        body: {
          spaceId: job.spaceId,
          senderId: job.senderId,
          content: job.content,
          ...(job.parentMessageId === undefined
            ? {}
            : { parentMessageId: job.parentMessageId }),
          ...(job.email === undefined ? {} : { email: job.email }),
          ...(job.telegram === undefined ? {} : { telegram: job.telegram }),
        },
      })
    );
  } catch (error) {
    return settleError(error);
  }
};

export const execute = internalAction({
  args: { rowId: v.id("outbox") },
  returns: v.null(),
  handler: async (ctx, args) => {
    const job = await ctx.runQuery(internal.outbox.loadJob, args);
    if (!job) {
      return null;
    }
    await ctx.runMutation(internal.outbox.settle, {
      rowId: args.rowId,
      ...(await run(job)),
    });
    return null;
  },
});
