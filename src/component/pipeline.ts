import type { Message } from "@photon-ai/api";
import type { FunctionHandle } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  type MutationCtx,
  mutation,
  query,
} from "./_generated/server.js";

/**
 * The batching pipeline.
 *
 * People text in bursts, so a reply per message can produce overlapping
 * answers that never see the actual question. A flush drains everything
 * outstanding for a space and hands it to the app as one turn.
 *
 * By default the flush is immediate and bursts are collapsed by *cancellation*
 * rather than by waiting: a follow-up supersedes the in-flight turn, carries
 * its messages into the next one, and cancels any reply that has not yet gone
 * out. An optional `debounceMs` adds a settling window on top, trading latency
 * for tighter batching.
 *
 * Two rules from Photon's production architecture are load-bearing here:
 *
 * 1. **Drain in the handler, not the enqueuer.** Rows stay in `inbound` until
 *    a flush actually runs. A cancelled flush therefore loses nothing — the
 *    next one picks the same rows up.
 * 2. **Cancellation is per-turn, not per-space.** Liveness is decided by
 *    turn identity, so a stale flag from a superseded turn can't orphan the
 *    turn that replaced it — and, unlike a timestamp comparison, it stays
 *    correct when both events land in the same millisecond.
 */

/**
 * No debounce by default.
 *
 * A fixed wait taxes every turn to solve a problem that only happens in
 * bursts. Cancellation already covers the burst case and costs nothing when
 * there is no burst: a follow-up message supersedes the in-flight turn,
 * carries its messages forward, and marks its queued sends cancelled — and
 * because a reply takes a moment to go out, a genuine burst almost always
 * lands inside that window and collapses anyway.
 *
 * Set `debounceMs` explicitly to trade latency for tighter batching.
 */
export const DEFAULT_DEBOUNCE_MS = 0;

/** Turn payload handed to the app's handler. */
export type TurnHandler = FunctionHandle<
  "action" | "mutation",
  {
    spaceId: string;
    turnId: string;
    messages: Message[];
    carried: Message[];
  },
  unknown
>;

/**
 * Whether an outbox row is a reply to its turn. A reaction is not: it
 * acknowledges a message rather than replying to it.
 */
export const isReply = (
  row: Pick<Doc<"outbox">, "kind" | "content">
): boolean =>
  row.kind === "sendMessage" &&
  (row.content as { type?: unknown } | undefined)?.type !== "reaction";

const getInflight = async (
  ctx: MutationCtx,
  spaceId: string
): Promise<Doc<"inflight"> | null> =>
  await ctx.db
    .query("inflight")
    .withIndex("by_spaceId", (q) => q.eq("spaceId", spaceId))
    .unique();

/**
 * Move a dead turn's messages into `carried` so the next batch still sees
 * them — the carry-forward rule.
 *
 * `includeAnswered` also rescues rows the handler already marked `handled`.
 * `completeTurn` runs when the handler *finishes*, but its reply is only
 * queued at that point — a supersession moments later cancels that reply. The
 * message would then be marked answered while its answer never shipped.
 */
const carryForwardDrained = async (
  ctx: MutationCtx,
  turnId: string,
  includeAnswered: boolean
): Promise<void> => {
  const drained = await ctx.db
    .query("inbound")
    .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
    .collect();
  for (const row of drained) {
    const spent = includeAnswered
      ? row.status !== "drained" && row.status !== "handled"
      : row.status !== "drained";
    if (spent) {
      continue;
    }
    await ctx.db.insert("carried", {
      spaceId: row.spaceId,
      messageId: row.messageId,
      message: row.message,
    });
    await ctx.db.patch("inbound", row._id, { status: "handled" as const });
  }
};

/**
 * Hand a dead turn's inherited messages back to the pool.
 *
 * These were carried in from an *earlier* cancelled turn, so they are not in
 * `inbound` under this turn and `carryForwardDrained` cannot see them.
 * Un-claiming makes the next flush pick them up again — without this, a
 * message is lost the moment two supersessions happen back to back.
 */
const releaseCarried = async (
  ctx: MutationCtx,
  turnId: string
): Promise<void> => {
  const claimed = await ctx.db
    .query("carried")
    .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
    .collect();
  for (const row of claimed) {
    await ctx.db.patch("carried", row._id, { turnId: undefined });
  }
};

/** Stop a dead turn's queued sends from going out. */
const cancelQueuedSends = async (
  ctx: MutationCtx,
  turnId: string
): Promise<void> => {
  const queued = await ctx.db
    .query("outbox")
    .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
    .collect();
  for (const row of queued) {
    if (row.status !== "pending") {
      continue;
    }
    if (row.jobId) {
      await ctx.scheduler.cancel(row.jobId);
    }
    await ctx.db.patch("outbox", row._id, { status: "cancelled" as const });
  }
};

/**
 * Cancel whatever turn currently owns this space.
 *
 * Cancels the scheduled flush/handler jobs, flags the turn, and moves any
 * rows the turn had already drained into `carried` so the next batch still
 * sees them. `ctx.scheduler.cancel` cannot stop an action that is already
 * running — that case is handled cooperatively by `isCancelled`, which a
 * running handler polls between steps.
 *
 * Returns the cancelled turn id, if there was one.
 */
export const cancelCurrentTurn = async (
  ctx: MutationCtx,
  spaceId: string
): Promise<string | undefined> => {
  const inflight = await getInflight(ctx, spaceId);
  if (!inflight) {
    return;
  }

  if (inflight.flushJobId) {
    await ctx.scheduler.cancel(inflight.flushJobId);
  }
  if (inflight.handlerJobId) {
    await ctx.scheduler.cancel(inflight.handlerJobId);
  }

  const turnId = inflight.turnId;
  if (turnId) {
    // A turn counts as answered once the API accepted a reply, or a reply is
    // already going out: a request in flight can't be recalled, and `claim`
    // finishes it rather than cancelling it.
    const queued = await ctx.db
      .query("outbox")
      .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
      .collect();
    const answered = queued.some(
      (row) =>
        (row.status === "sent" || row.status === "sending") && isReply(row)
    );

    await carryForwardDrained(ctx, turnId, !answered);
    await releaseCarried(ctx, turnId);
    await cancelQueuedSends(ctx, turnId);

    const record = await ctx.db
      .query("turns")
      .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
      .unique();
    if (record) {
      await ctx.db.patch("turns", record._id, { status: "cancelled" as const });
    }
  }

  // Forget the dead turn: a second message that lands before the next flush
  // would otherwise cancel it again and carry its messages twice.
  await ctx.db.patch("inflight", inflight._id, {
    turnId: undefined,
    flushJobId: undefined,
    handlerJobId: undefined,
  });
  return turnId;
};

/** Arm (or re-arm) the debounce timer for a space. */
export const scheduleFlush = async (
  ctx: MutationCtx,
  args: {
    spaceId: string;
    debounceMs: number;
    onTurn?: string;
    only?: Id<"inbound">;
  }
): Promise<void> => {
  const jobId = await ctx.scheduler.runAfter(
    args.debounceMs,
    internal.pipeline.flush,
    {
      spaceId: args.spaceId,
      ...(args.onTurn === undefined ? {} : { onTurn: args.onTurn }),
      ...(args.only === undefined ? {} : { only: args.only }),
    }
  );

  // In immediate mode each message owns its own flush, so the job id is not
  // tracked for cancellation — there is nothing to supersede.
  if (args.only) {
    return;
  }

  const inflight = await getInflight(ctx, args.spaceId);
  if (inflight) {
    await ctx.db.patch("inflight", inflight._id, { flushJobId: jobId });
    return;
  }
  await ctx.db.insert("inflight", { spaceId: args.spaceId, flushJobId: jobId });
};

/**
 * Debounce expired: drain the burst and start a turn.
 *
 * Carried messages come first so the app can present them as earlier context
 * rather than fresh input.
 */
export const flush = internalMutation({
  args: {
    spaceId: v.string(),
    onTurn: v.optional(v.string()),
    /**
     * Drain exactly this row and nothing else — `immediate` mode, where every
     * message is its own turn. Omitted in `collapse` mode, which drains
     * everything outstanding for the space.
     */
    only: v.optional(v.id("inbound")),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const target = args.only ? await ctx.db.get("inbound", args.only) : null;
    if (args.only && target?.status !== "queued") {
      // Already drained by an earlier flush, or superseded.
      return null;
    }

    const queued = target
      ? [target]
      : await ctx.db
          .query("inbound")
          .withIndex("by_spaceId_and_status", (q) =>
            q.eq("spaceId", args.spaceId).eq("status", "queued")
          )
          .collect();
    // Inherited context only exists in collapse mode; immediate turns are
    // strictly one message.
    const carriedRows = args.only
      ? []
      : await ctx.db
          .query("carried")
          .withIndex("by_spaceId", (q) => q.eq("spaceId", args.spaceId))
          .collect();

    if (queued.length === 0 && carriedRows.length === 0) {
      return null;
    }

    const now = Date.now();
    // Unique per turn, and derived from data already in the transaction —
    // component functions are deterministic, so no Math.random() here.
    const turnId = `${args.spaceId}:${now}:${queued[0]?._id ?? "carried"}`;

    for (const row of queued) {
      await ctx.db.patch("inbound", row._id, {
        status: "drained" as const,
        turnId,
      });
    }
    // Claim rather than delete: if this turn is itself superseded before it
    // answers, `cancelCurrentTurn` releases these back for the next flush.
    // Deleting here would drop them on a second consecutive supersession.
    const carried = carriedRows.map((row) => row.message);
    for (const row of carriedRows) {
      await ctx.db.patch("carried", row._id, { turnId });
    }

    await ctx.db.insert("turns", {
      turnId,
      spaceId: args.spaceId,
      status: "live" as const,
    });

    // `inflight` models the single current turn, which only makes sense when
    // turns supersede each other. Immediate turns are independent, so they are
    // deliberately not registered there.
    if (!args.only) {
      const inflight = await getInflight(ctx, args.spaceId);
      const inflightId =
        inflight?._id ??
        (await ctx.db.insert("inflight", { spaceId: args.spaceId }));
      await ctx.db.patch("inflight", inflightId, {
        turnId,
        flushJobId: undefined,
      });
    }

    if (!args.onTurn) {
      // No handler wired up: the messages are still durable and marked
      // drained, so a later turn carries them forward rather than losing them.
      return null;
    }

    const handlerJobId = await ctx.scheduler.runAfter(
      0,
      args.onTurn as TurnHandler,
      {
        spaceId: args.spaceId,
        turnId,
        messages: queued.map((row) => row.message),
        carried,
      }
    );
    // Only collapse turns are cancellable, so only they track the handler job.
    if (!args.only) {
      const inflight = await getInflight(ctx, args.spaceId);
      if (inflight) {
        await ctx.db.patch("inflight", inflight._id, { handlerJobId });
      }
    }
    return null;
  },
});

/**
 * Has this turn been superseded?
 *
 * Read from the turn's own record, not from "is this the space's current
 * turn?". In immediate mode several turns are open at once and are
 * independent, so identity-against-current would let each new message
 * invalidate its predecessors. Explicit per-turn state is also immune to two
 * events landing in the same millisecond.
 *
 * A long-running handler should poll this between steps (before generating,
 * before sending) and bail out when it returns true — `ctx.scheduler.cancel`
 * cannot stop an action that is already running.
 */
export const isCancelled = query({
  args: { spaceId: v.string(), turnId: v.string() },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const record = await ctx.db
      .query("turns")
      .withIndex("by_turnId", (q) => q.eq("turnId", args.turnId))
      .unique();
    // An unknown turn is treated as dead — a stale handler must not speak.
    if (!record) {
      return true;
    }
    return record.status === "cancelled";
  },
});

/** Mark a turn's drained messages handled. Called when a turn completes. */
export const completeTurn = mutation({
  args: { spaceId: v.string(), turnId: v.string() },
  returns: v.null(),
  handler: async (ctx, args) => {
    const rows = await ctx.db
      .query("inbound")
      .withIndex("by_turnId", (q) => q.eq("turnId", args.turnId))
      .collect();
    for (const row of rows) {
      if (row.status === "drained") {
        await ctx.db.patch("inbound", row._id, { status: "handled" as const });
      }
    }
    // Inherited messages are deliberately NOT dropped here. The handler has
    // only *queued* its reply at this point; a supersession moments later
    // cancels it. They are discarded in `outbox.settle` once a reply actually
    // ships — the same rule that governs `inbound` rows.

    const inflight = await getInflight(ctx, args.spaceId);
    if (inflight?.turnId === args.turnId) {
      await ctx.db.patch("inflight", inflight._id, { handlerJobId: undefined });
    }
    return null;
  },
});
