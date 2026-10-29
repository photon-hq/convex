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
 * The turn pipeline. In `immediate` mode every message is its own turn. In
 * `collapse` mode a flush drains everything queued for a space into one turn,
 * and a new message supersedes the turn in flight: it cancels the turn's
 * unsent replies and carries its messages into the next one.
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
 * Move a dead turn's messages into `carried`, so the next turn still sees
 * them. `includeHandled` also takes those `completeTurn` marked handled: when
 * the turn was not answered, the reply it queued is being cancelled.
 */
const carryForwardDrained = async (
  ctx: MutationCtx,
  drained: Doc<"inbound">[],
  includeHandled: boolean
): Promise<void> => {
  for (const row of drained) {
    const spent = includeHandled
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
 * Settle the messages a dead turn inherited, which `carryForwardDrained` does
 * not see: drop them if the turn spent them, else release them to the next
 * flush.
 */
const releaseCarried = async (
  ctx: MutationCtx,
  turnId: Id<"turns">,
  spent: boolean
): Promise<void> => {
  const claimed = await ctx.db
    .query("carried")
    .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
    .collect();
  for (const row of claimed) {
    if (spent) {
      await ctx.db.delete("carried", row._id);
    } else {
      await ctx.db.patch("carried", row._id, { turnId: undefined });
    }
  }
};

/** Stop a dead turn's queued sends, given all of its outbox rows. */
const cancelQueuedSends = async (
  ctx: MutationCtx,
  rows: Doc<"outbox">[]
): Promise<void> => {
  for (const row of rows) {
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
 * Cancel the turn that owns this space, if any, and record `flushJobId`, the
 * flush that starts the next turn, in its place. A handler that is already
 * running finds out through `isCancelled`.
 */
export const cancelCurrentTurn = async (
  ctx: MutationCtx,
  spaceId: string,
  flushJobId?: Id<"_scheduled_functions">
): Promise<void> => {
  const inflight = await getInflight(ctx, spaceId);
  if (!inflight) {
    if (flushJobId) {
      await ctx.db.insert("inflight", { spaceId, flushJobId });
    }
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
    // already going out: a request in flight can't be recalled, and
    // `cancelQueuedSends` leaves it to finish, with its retries.
    const queued = await ctx.db
      .query("outbox")
      .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
      .collect();
    const answered = queued.some(
      (row) =>
        (row.status === "sent" || row.status === "sending") && isReply(row)
    );
    const drained = await ctx.db
      .query("inbound")
      .withIndex("by_turnId", (q) => q.eq("turnId", turnId))
      .collect();

    await carryForwardDrained(ctx, drained, !answered);
    // Inherited messages go the way of the turn's own: spent once the turn was
    // answered and completed (no message left `drained`), so they are not
    // answered twice, and handed on otherwise.
    const completed = drained.every((row) => row.status !== "drained");
    await releaseCarried(ctx, turnId, answered && completed);
    await cancelQueuedSends(ctx, queued);
    await ctx.db.patch("turns", turnId, { status: "cancelled" as const });
  }

  // Forget the dead turn: a second message that lands before the next flush
  // would otherwise cancel it again and carry its messages twice.
  await ctx.db.patch("inflight", inflight._id, {
    turnId: undefined,
    flushJobId,
    handlerJobId: undefined,
  });
};

/**
 * Schedule a flush after the debounce window. `cancelCurrentTurn` records a
 * collapse flush, so the next message can cancel it.
 */
export const scheduleFlush = async (
  ctx: MutationCtx,
  args: {
    spaceId: string;
    debounceMs: number;
    onTurn?: string;
    only?: Id<"inbound">;
  }
): Promise<Id<"_scheduled_functions">> =>
  await ctx.scheduler.runAfter(args.debounceMs, internal.pipeline.flush, {
    spaceId: args.spaceId,
    onTurn: args.onTurn,
    only: args.only,
  });

/** Drain a space's queued messages into a new turn and hand it to the app. */
export const flush = internalMutation({
  args: {
    spaceId: v.string(),
    onTurn: v.optional(v.string()),
    // Immediate mode: drain this row alone. Collapse mode drains every queued
    // row of the space.
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
    // Only collapse turns inherit messages.
    const carriedRows = args.only
      ? []
      : await ctx.db
          .query("carried")
          .withIndex("by_spaceId", (q) => q.eq("spaceId", args.spaceId))
          .collect();

    if (queued.length === 0 && carriedRows.length === 0) {
      return null;
    }

    const turnId = await ctx.db.insert("turns", { status: "live" as const });

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

    const handlerJobId = args.onTurn
      ? await ctx.scheduler.runAfter(0, args.onTurn as TurnHandler, {
          spaceId: args.spaceId,
          turnId,
          messages: queued.map((row) => row.message),
          carried,
        })
      : undefined;

    // Only collapse turns supersede each other, so only they own `inflight`.
    if (!args.only) {
      const inflight = await getInflight(ctx, args.spaceId);
      if (inflight) {
        await ctx.db.patch("inflight", inflight._id, {
          turnId,
          flushJobId: undefined,
          handlerJobId,
        });
      } else {
        await ctx.db.insert("inflight", {
          spaceId: args.spaceId,
          turnId,
          handlerJobId,
        });
      }
    }
    return null;
  },
});

/** Has this turn been superseded? */
export const isCancelled = query({
  args: { turnId: v.id("turns") },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const record = await ctx.db.get("turns", args.turnId);
    // An unknown turn is treated as dead — a stale handler must not speak.
    if (!record) {
      return true;
    }
    return record.status === "cancelled";
  },
});

/** Mark a turn's drained messages handled. Called when a turn completes. */
export const completeTurn = mutation({
  args: { spaceId: v.string(), turnId: v.id("turns") },
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
    // Inherited messages stay until a reply actually ships (`outbox.settle`):
    // so far the reply is only queued, and a supersession can still cancel it.

    const inflight = await getInflight(ctx, args.spaceId);
    if (inflight?.turnId === args.turnId) {
      await ctx.db.patch("inflight", inflight._id, { handlerJobId: undefined });
    }
    return null;
  },
});
