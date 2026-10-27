import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  internalQuery,
  type MutationCtx,
  mutation,
  query,
} from "./_generated/server.js";
import { isReply } from "./pipeline.js";

/**
 * The outbound half: a durable outbox, one row per action.
 *
 * A row's id is its idempotency key. `sendMessage` and `markMessageRead` send
 * it as the request's `idempotency-key` header, and every retry of the row
 * reuses it. So when a request succeeded but its response was lost, the retry
 * gets the original result back instead of sending the message a second time.
 */

const MAX_ATTEMPTS = 5;
const BASE_RETRY_DELAY_MS = 1000;
const RETRY_BACKOFF_FACTOR = 4;
// When `recover` first looks at an attempt, and how often after that while
// the attempt still runs. An attempt is one request, which the Photon client
// abandons after 30 s, so a healthy attempt has settled by the first look.
const RECOVER_DELAY_MS = 60_000;

export const sendKind = v.union(
  v.literal("sendMessage"),
  v.literal("markMessageRead"),
  v.literal("setSpaceTyping")
);

const turnIsLive = async (
  ctx: MutationCtx,
  turnId: string | undefined
): Promise<boolean> => {
  if (!turnId) {
    // A send with no turn (e.g. a proactive outbound message) is never
    // subject to conversational cancellation.
    return true;
  }
  const record = await ctx.db
    .query("turns")
    .withIndex("by_turn", (q) => q.eq("turnId", turnId))
    .unique();
  if (!record) {
    return false;
  }
  return record.status === "live";
};

/** Keep the single `config` row in step with the credentials the app passed. */
const saveConfig = async (
  ctx: MutationCtx,
  config: { projectId: string; apiKey: string; baseUrl?: string }
): Promise<void> => {
  const existing = await ctx.db.query("config").unique();
  if (!existing) {
    await ctx.db.insert("config", config);
    return;
  }
  if (
    existing.projectId !== config.projectId ||
    existing.apiKey !== config.apiKey ||
    existing.baseUrl !== config.baseUrl
  ) {
    await ctx.db.replace(existing._id, config);
  }
};

/**
 * Schedule a new row's claim. A reply queued behind one that has not settled
 * waits instead, keeping its `pacingMs` for `sendNextReply`. Read receipts,
 * typing and reactions are never held back.
 */
const scheduleClaim = async (
  ctx: MutationCtx,
  rowId: Id<"outbox">,
  args: Pick<Doc<"outbox">, "kind" | "content" | "pacingMs">,
  priorForTurn: Doc<"outbox">[]
): Promise<void> => {
  const previous = isReply(args)
    ? priorForTurn.filter(isReply).at(-1)
    : undefined;
  if (previous?.status === "pending" || previous?.status === "sending") {
    if (args.pacingMs) {
      await ctx.db.patch(rowId, { pacingMs: args.pacingMs });
    }
    return;
  }
  const jobId = await ctx.scheduler.runAfter(
    previous ? (args.pacingMs ?? 0) : 0,
    internal.outbox.claim,
    { rowId }
  );
  await ctx.db.patch(rowId, { jobId });
};

/**
 * Once a reply settles, send the next reply its turn queued behind it,
 * `pacingMs` later.
 */
const sendNextReply = async (
  ctx: MutationCtx,
  row: Doc<"outbox">
): Promise<void> => {
  if (!(row.turnId && isReply(row))) {
    return;
  }
  const next = (
    await ctx.db
      .query("outbox")
      .withIndex("by_turn", (q) => q.eq("turnId", row.turnId))
      .collect()
  ).find(
    (other) =>
      other.status === "pending" && other.jobId === undefined && isReply(other)
  );
  if (!next) {
    return;
  }
  const jobId = await ctx.scheduler.runAfter(
    next.pacingMs ?? 0,
    internal.outbox.claim,
    { rowId: next._id }
  );
  await ctx.db.patch(next._id, { jobId });
};

/**
 * Queue one outbound action.
 *
 * Rejects (without writing) when the queueing turn has already been
 * superseded — the guard that stops a cancelled turn from talking over the
 * turn that replaced it. A turn's replies go out one at a time, in the order
 * they were queued, each at least `pacingMs` after the one before it.
 */
export const enqueue = mutation({
  args: {
    spaceId: v.string(),
    turnId: v.optional(v.string()),
    kind: sendKind,
    content: v.optional(v.any()),
    parentMessageId: v.optional(v.string()),
    email: v.optional(v.any()),
    telegram: v.optional(v.any()),
    targetMessageId: v.optional(v.string()),
    pacingMs: v.optional(v.number()),
    // Photon API credentials. Given `projectId` and `apiKey`, the three
    // together replace the stored ones when different, so an omitted `baseUrl`
    // clears the stored one; without them, the stored ones are used.
    projectId: v.optional(v.string()),
    apiKey: v.optional(v.string()),
    baseUrl: v.optional(v.string()),
  },
  returns: v.object({
    queued: v.boolean(),
    idempotencyKey: v.optional(v.string()),
    reason: v.optional(v.string()),
  }),
  handler: async (ctx, args) => {
    if (args.projectId !== undefined && args.apiKey !== undefined) {
      await saveConfig(ctx, {
        projectId: args.projectId,
        apiKey: args.apiKey,
        ...(args.baseUrl === undefined ? {} : { baseUrl: args.baseUrl }),
      });
    }

    if (!(await turnIsLive(ctx, args.turnId))) {
      return { queued: false, reason: "turn-cancelled" };
    }

    const priorForTurn = args.turnId
      ? await ctx.db
          .query("outbox")
          .withIndex("by_turn", (q) => q.eq("turnId", args.turnId))
          .collect()
      : [];

    // The read endpoint marks one message read, so fall back to the newest
    // inbound message of this turn when the caller did not name one.
    let targetMessageId = args.targetMessageId;
    if (!targetMessageId && args.kind === "markMessageRead" && args.turnId) {
      const newest = await ctx.db
        .query("inbound")
        .withIndex("by_turn", (q) => q.eq("turnId", args.turnId))
        .collect();
      targetMessageId = newest.at(-1)?.messageId;
    }

    const rowId = await ctx.db.insert("outbox", {
      spaceId: args.spaceId,
      ...(args.turnId === undefined ? {} : { turnId: args.turnId }),
      kind: args.kind,
      ...(args.content === undefined ? {} : { content: args.content }),
      ...(args.parentMessageId === undefined
        ? {}
        : { parentMessageId: args.parentMessageId }),
      ...(args.email === undefined ? {} : { email: args.email }),
      ...(args.telegram === undefined ? {} : { telegram: args.telegram }),
      ...(targetMessageId === undefined ? {} : { targetMessageId }),
      status: "pending" as const,
      attempts: 0,
      queuedAt: Date.now(),
    });
    await scheduleClaim(ctx, rowId, args, priorForTurn);

    return { queued: true, idempotencyKey: rowId };
  },
});

/**
 * Take one pending row and hand it to `transport.execute`, with `recover` to
 * follow up in case the action stops before settling it.
 *
 * Only the row id is scheduled: the action reads the row and the credentials
 * itself (`loadJob`), so the API key never appears in a scheduled function's
 * arguments.
 */
export const claim = internalMutation({
  args: { rowId: v.id("outbox") },
  returns: v.null(),
  handler: async (ctx, args) => {
    const row = await ctx.db.get(args.rowId);
    if (row?.status !== "pending") {
      return null;
    }
    // Only a row that never went out is cancelled with its turn. One that
    // was attempted may already have reached the API, and a reply in flight
    // counted as its turn's answer, so it is finished with retries.
    if (row.attempts === 0 && !(await turnIsLive(ctx, row.turnId))) {
      await ctx.db.patch(row._id, { status: "cancelled" as const });
      return null;
    }

    const attempt = row.attempts + 1;
    await ctx.db.patch(row._id, {
      status: "sending" as const,
      attempts: attempt,
    });
    const jobId = await ctx.scheduler.runAfter(0, internal.transport.execute, {
      rowId: row._id,
    });
    await ctx.scheduler.runAfter(RECOVER_DELAY_MS, internal.outbox.recover, {
      rowId: row._id,
      attempt,
      jobId,
    });
    return null;
  },
});

/** Everything `transport.execute` needs for one row. */
export const loadJob = internalQuery({
  args: { rowId: v.id("outbox") },
  returns: v.union(
    v.null(),
    v.object({
      spaceId: v.string(),
      idempotencyKey: v.string(),
      kind: sendKind,
      content: v.optional(v.any()),
      parentMessageId: v.optional(v.string()),
      email: v.optional(v.any()),
      telegram: v.optional(v.any()),
      targetMessageId: v.optional(v.string()),
      senderId: v.optional(v.string()),
      config: v.union(
        v.null(),
        v.object({
          projectId: v.string(),
          apiKey: v.string(),
          baseUrl: v.optional(v.string()),
        })
      ),
    })
  ),
  handler: async (ctx, args) => {
    const row = await ctx.db.get(args.rowId);
    if (!row) {
      return null;
    }
    const space = await ctx.db
      .query("spaces")
      .withIndex("by_space", (q) => q.eq("spaceId", row.spaceId))
      .unique();
    const config = await ctx.db.query("config").unique();
    return {
      spaceId: row.spaceId,
      idempotencyKey: row._id,
      kind: row.kind,
      ...(row.content === undefined ? {} : { content: row.content }),
      ...(row.parentMessageId === undefined
        ? {}
        : { parentMessageId: row.parentMessageId }),
      ...(row.email === undefined ? {} : { email: row.email }),
      ...(row.telegram === undefined ? {} : { telegram: row.telegram }),
      ...(row.targetMessageId === undefined
        ? {}
        : { targetMessageId: row.targetMessageId }),
      ...(space?.senderId === undefined ? {} : { senderId: space.senderId }),
      config: config
        ? {
            projectId: config.projectId,
            apiKey: config.apiKey,
            ...(config.baseUrl === undefined
              ? {}
              : { baseUrl: config.baseUrl }),
          }
        : null,
    };
  },
});

/**
 * After a failed attempt, retry the row with backoff, or fail it when the
 * error is final or the attempts are used up.
 */
const retryOrFail = async (
  ctx: MutationCtx,
  row: Doc<"outbox">,
  failure: { error?: string; retryable?: boolean }
): Promise<void> => {
  const exhausted = row.attempts >= MAX_ATTEMPTS;
  if (exhausted || failure.retryable === false) {
    await ctx.db.patch(row._id, {
      status: "failed" as const,
      ...(failure.error === undefined ? {} : { lastError: failure.error }),
    });
    await sendNextReply(ctx, row);
    return;
  }

  // Same idempotency key on the retry, so a `sendMessage` or
  // `markMessageRead` that actually landed before the error is answered with
  // its original result instead of being carried out twice.
  const delay = BASE_RETRY_DELAY_MS * RETRY_BACKOFF_FACTOR ** row.attempts;
  const jobId = await ctx.scheduler.runAfter(delay, internal.outbox.claim, {
    rowId: row._id,
  });
  await ctx.db.patch(row._id, {
    status: "pending" as const,
    jobId,
    ...(failure.error === undefined ? {} : { lastError: failure.error }),
  });
};

/** Record the API's verdict for one row; retries with backoff on failure. */
export const settle = internalMutation({
  args: {
    rowId: v.id("outbox"),
    ok: v.boolean(),
    messageId: v.optional(v.string()),
    error: v.optional(v.string()),
    retryable: v.optional(v.boolean()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const row = await ctx.db.get(args.rowId);
    if (!row) {
      return null;
    }
    if (args.ok) {
      await ctx.db.patch(row._id, {
        status: "sent" as const,
        sentAt: Date.now(),
        ...(args.messageId === undefined ? {} : { messageId: args.messageId }),
      });

      // The API accepted a reply, so this turn's inherited messages are
      // finally spent. Doing it here rather than when the handler finishes is
      // what makes the pipeline lossless: until a reply goes out, every
      // message stays recoverable by the next turn.
      if (isReply(row) && row.turnId) {
        const inherited = await ctx.db
          .query("carried")
          .withIndex("by_turn", (q) => q.eq("turnId", row.turnId))
          .collect();
        for (const spent of inherited) {
          await ctx.db.delete(spent._id);
        }
      }
      await sendNextReply(ctx, row);
      return null;
    }

    await retryOrFail(ctx, row, args);
    return null;
  },
});

/**
 * Retry an attempt whose `transport.execute` stopped before settling it.
 *
 * Convex does not retry an action that fails, for example when `runQuery` or
 * `runMutation` throws, so without this the row would stay `sending` and never
 * go out. While the action is still pending or running, look again later.
 */
export const recover = internalMutation({
  args: {
    rowId: v.id("outbox"),
    attempt: v.number(),
    jobId: v.id("_scheduled_functions"),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const row = await ctx.db.get(args.rowId);
    if (row?.status !== "sending" || row.attempts !== args.attempt) {
      return null;
    }
    const job = await ctx.db.system.get(args.jobId);
    if (job?.state.kind === "pending" || job?.state.kind === "inProgress") {
      await ctx.scheduler.runAfter(
        RECOVER_DELAY_MS,
        internal.outbox.recover,
        args
      );
      return null;
    }
    const reason = job?.state.kind === "failed" ? `: ${job.state.error}` : "";
    await retryOrFail(ctx, row, {
      error: `transport.execute stopped before settling the row${reason}`,
    });
    return null;
  },
});

const outboxRow = v.object({
  idempotencyKey: v.string(),
  kind: v.string(),
  status: v.string(),
  attempts: v.number(),
  queuedAt: v.number(),
  sentAt: v.optional(v.number()),
  messageId: v.optional(v.string()),
  lastError: v.optional(v.string()),
});

const toOutboxRow = (row: Doc<"outbox">) => ({
  idempotencyKey: row._id,
  kind: row.kind,
  status: row.status,
  attempts: row.attempts,
  queuedAt: row.queuedAt,
  ...(row.sentAt === undefined ? {} : { sentAt: row.sentAt }),
  ...(row.messageId === undefined ? {} : { messageId: row.messageId }),
  ...(row.lastError === undefined ? {} : { lastError: row.lastError }),
});

/**
 * Recent outbox rows for a space, newest last — delivery status and the
 * failure audit trail.
 */
export const list = query({
  args: { spaceId: v.string(), limit: v.optional(v.number()) },
  returns: v.array(outboxRow),
  handler: async (ctx, args) => {
    const rows = await ctx.db
      .query("outbox")
      .withIndex("by_space_queued", (q) => q.eq("spaceId", args.spaceId))
      .order("desc")
      .take(args.limit ?? 50);
    return rows.reverse().map(toOutboxRow);
  },
});
