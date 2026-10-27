import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Doc } from "./_generated/dataModel.js";
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
 * Every row carries an `idempotencyKey` derived from `(turnId, seq)`, or for
 * a send without a turn, from the space and the millisecond it was queued.
 * `sendMessage` and `markMessageRead` send it as the request's
 * `idempotency-key` header, and every retry of a row reuses it. So when a
 * request succeeded but its response was lost, the retry gets the original
 * result back instead of sending the message a second time.
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

const isControlAction = (kind: Doc<"outbox">["kind"]): boolean =>
  kind === "markMessageRead" || kind === "setSpaceTyping";

/**
 * Pacing delays each reply after the first one in a turn by `pacingMs`,
 * counted from when it is queued. It must never delay the first one: that is
 * pure latency before the user sees anything.
 *
 * Deliberately counted over replies only, not `seq`. Control actions (read
 * receipts, typing) and reactions occupy sequence numbers too, so keying off
 * `seq === 0` would let an auto-markMessageRead, or a reaction sent first,
 * push the actual reply into a paced slot and reintroduce the delay this
 * avoids. Neither is ever delayed itself.
 */
const pacingDelay = (
  args: { kind: Doc<"outbox">["kind"]; content?: unknown; pacingMs?: number },
  priorForTurn: Doc<"outbox">[]
): number => {
  const isReaction =
    (args.content as { type?: unknown } | undefined)?.type === "reaction";
  if (isControlAction(args.kind) || isReaction) {
    return 0;
  }
  const priorReplies = priorForTurn.filter(isReply).length;
  return priorReplies > 0 ? (args.pacingMs ?? 0) : 0;
};

/**
 * Queue one outbound action.
 *
 * Rejects (without writing) when the queueing turn has already been
 * superseded — the guard that stops a cancelled turn from talking over the
 * turn that replaced it. `pacingMs` staggers a multi-message reply so it reads
 * like typing rather than a wall of text arriving at once.
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
    const now = Date.now();
    // Without a turn, `seq` counts this space's rows of the same millisecond.
    // `Date.now()` is fixed for a whole mutation, so two sends in one mutation
    // would otherwise share a key, and the second would be dropped as a
    // duplicate.
    const seq = args.turnId
      ? priorForTurn.length
      : (
          await ctx.db
            .query("outbox")
            .withIndex("by_space_queued", (q) =>
              q.eq("spaceId", args.spaceId).eq("queuedAt", now)
            )
            .collect()
        ).length;
    // One key per row; every retry of the row reuses it.
    const idempotencyKey = `${args.turnId ?? `${args.spaceId}:${now}`}#${seq}`;

    const existing = await ctx.db
      .query("outbox")
      .withIndex("by_idempotency_key", (q) =>
        q.eq("idempotencyKey", idempotencyKey)
      )
      .unique();
    if (existing) {
      return { queued: false, idempotencyKey, reason: "duplicate" };
    }

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
      seq,
      idempotencyKey,
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
      queuedAt: now,
    });

    const jobId = await ctx.scheduler.runAfter(
      pacingDelay(args, priorForTurn),
      internal.outbox.claim,
      { rowId }
    );
    await ctx.db.patch(rowId, { jobId });

    return { queued: true, idempotencyKey };
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
    if (!(await turnIsLive(ctx, row.turnId))) {
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
      idempotencyKey: row.idempotencyKey,
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
      // what makes the pipeline lossless: until a reply is accepted, every
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
  idempotencyKey: row.idempotencyKey,
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
