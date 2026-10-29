import { v } from "convex/values";
import { type MutationCtx, mutation, query } from "./_generated/server.js";
import {
  cancelCurrentTurn,
  DEFAULT_DEBOUNCE_MS,
  scheduleFlush,
} from "./pipeline.js";

/** Inbound: dedupe, the queue that flushes drain, and the space registry. */

const messageArgs = {
  spaceId: v.string(),
  messageId: v.string(),
  platform: v.string(),
  // The Photon `Message` (`data.object` of the event).
  message: v.any(),
  triggersTurn: v.optional(v.boolean()),
  senderId: v.optional(v.string()),
  kind: v.optional(v.string()),
};

const upsertSpace = async (
  ctx: MutationCtx,
  args: {
    spaceId: string;
    platform: string;
    senderId?: string;
    kind?: string;
  }
): Promise<void> => {
  const existing = await ctx.db
    .query("spaces")
    .withIndex("by_spaceId", (q) => q.eq("spaceId", args.spaceId))
    .unique();
  const facts = {
    platform: args.platform,
    // Routing facts are sticky: a later delivery without `receivedBy` must not
    // erase the sender we already learned, or replies to this space would
    // have no `senderId` to send as.
    ...(args.senderId === undefined ? {} : { senderId: args.senderId }),
    ...(args.kind === undefined ? {} : { kind: args.kind }),
  };
  if (!existing) {
    await ctx.db.insert("spaces", { spaceId: args.spaceId, ...facts });
    return;
  }
  // Most deliveries repeat what the space already knows; write only a change.
  const changed = (Object.keys(facts) as (keyof typeof facts)[]).some(
    (key) => existing[key] !== facts[key]
  );
  if (changed) {
    await ctx.db.patch("spaces", existing._id, facts);
  }
};

/**
 * Store one verified inbound message and schedule its turn. A redelivery
 * (deliveries are at-least-once) changes nothing and returns `deduped: true`.
 */
export const enqueue = mutation({
  args: {
    ...messageArgs,
    debounceMs: v.optional(v.number()),
    mode: v.optional(v.union(v.literal("immediate"), v.literal("collapse"))),
    // The app's turn handler, as a function handle. Passed with every delivery
    // rather than stored, so the app can change it without a migration.
    onTurn: v.optional(v.string()),
  },
  returns: v.object({ deduped: v.boolean() }),
  handler: async (ctx, args) => {
    const duplicate = await ctx.db
      .query("inbound")
      .withIndex("by_messageId", (q) => q.eq("messageId", args.messageId))
      .unique();
    if (duplicate) {
      return { deduped: true };
    }

    const now = Date.now();
    await upsertSpace(ctx, {
      spaceId: args.spaceId,
      platform: args.platform,
      senderId: args.senderId,
      kind: args.kind,
    });

    // Signals (read receipts, typing) are stored already handled, so no flush
    // drains them.
    const triggersTurn = args.triggersTurn ?? true;

    const rowId = await ctx.db.insert("inbound", {
      spaceId: args.spaceId,
      messageId: args.messageId,
      message: args.message,
      queuedAt: now,
      status: triggersTurn ? ("queued" as const) : ("handled" as const),
    });

    if (!triggersTurn) {
      // A read receipt must not cancel, or delay, the reply it acknowledges.
      return { deduped: false };
    }

    if ((args.mode ?? "immediate") === "immediate") {
      // Every message is its own turn, and supersedes nothing.
      await scheduleFlush(ctx, {
        spaceId: args.spaceId,
        debounceMs: args.debounceMs ?? DEFAULT_DEBOUNCE_MS,
        onTurn: args.onTurn,
        only: rowId,
      });
      return { deduped: false };
    }

    // Collapse mode: the new message supersedes the turn in flight.
    const flushJobId = await scheduleFlush(ctx, {
      spaceId: args.spaceId,
      debounceMs: args.debounceMs ?? DEFAULT_DEBOUNCE_MS,
      onTurn: args.onTurn,
    });
    await cancelCurrentTurn(ctx, args.spaceId, flushJobId);
    return { deduped: false };
  },
});

/** A space's routing record and the time of its newest message, or `null`. */
export const getSpace = query({
  args: { spaceId: v.string() },
  returns: v.union(
    v.null(),
    v.object({
      spaceId: v.string(),
      platform: v.string(),
      senderId: v.optional(v.string()),
      kind: v.optional(v.string()),
      lastInboundAt: v.number(),
    })
  ),
  handler: async (ctx, args) => {
    const space = await ctx.db
      .query("spaces")
      .withIndex("by_spaceId", (q) => q.eq("spaceId", args.spaceId))
      .unique();
    if (!space) {
      return null;
    }
    // Read off the newest stored message, so that a delivery does not have to
    // write the space just to record its time.
    const newest = await ctx.db
      .query("inbound")
      .withIndex("by_spaceId_and_queuedAt", (q) =>
        q.eq("spaceId", args.spaceId)
      )
      .order("desc")
      .first();
    return {
      spaceId: space.spaceId,
      platform: space.platform,
      senderId: space.senderId,
      kind: space.kind,
      lastInboundAt: newest?.queuedAt ?? space._creationTime,
    };
  },
});

/** Recent messages for a space, newest last. Intended for dashboards. */
export const listMessages = query({
  args: {
    spaceId: v.string(),
    limit: v.optional(v.number()),
  },
  returns: v.array(
    v.object({
      messageId: v.string(),
      message: v.any(),
      queuedAt: v.number(),
      status: v.string(),
    })
  ),
  handler: async (ctx, args) => {
    const rows = await ctx.db
      .query("inbound")
      .withIndex("by_spaceId_and_queuedAt", (q) =>
        q.eq("spaceId", args.spaceId)
      )
      .order("desc")
      .take(args.limit ?? 50);
    return rows.reverse().map((row) => ({
      messageId: row.messageId,
      message: row.message,
      queuedAt: row.queuedAt,
      status: row.status,
    }));
  },
});
