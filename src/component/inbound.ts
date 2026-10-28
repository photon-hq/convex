import { v } from "convex/values";
import { type MutationCtx, mutation, query } from "./_generated/server.js";
import {
  cancelCurrentTurn,
  DEFAULT_DEBOUNCE_MS,
  scheduleFlush,
} from "./pipeline.js";

/**
 * The inbound half of the pipeline: durable dedupe, the batch queue, and the
 * space registry.
 *
 * The HTTP entry point verifies a delivery and calls `enqueue` before it
 * answers 200, so a message is durable the moment Photon is told the delivery
 * succeeded. Nothing can be left for after the response: on a serverless
 * runtime the isolate is frozen the instant the response is returned.
 */

const messageArgs = {
  spaceId: v.string(),
  messageId: v.string(),
  platform: v.string(),
  // The Photon `Message` (`data.object` of the event).
  message: v.any(),
  raw: v.optional(v.any()),
  triggersTurn: v.optional(v.boolean()),
  // The agent's own user in this space (`receivedBy.id`); replies send as it.
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
 * Record one verified inbound message and (re)arm the debounce window.
 *
 * Returns `{ deduped: true }` when the message id has been seen before —
 * webhook deliveries are at-least-once, so a redelivery must not create a
 * second queue row or restart the debounce timer.
 */
export const enqueue = mutation({
  args: {
    ...messageArgs,
    debounceMs: v.optional(v.number()),
    /**
     * `"immediate"` (default): every message is its own turn — no supersession,
     * no carry-forward, no batching. `"collapse"`: a new message supersedes the
     * in-flight turn and its messages are carried into the next one.
     */
    mode: v.optional(v.union(v.literal("immediate"), v.literal("collapse"))),
    // Function handle (`createFunctionHandle(internal.foo.bar)`) for the app's
    // turn handler. Threaded through per-call rather than stored at install
    // time so the app can change or version its handler without a migration.
    onTurn: v.optional(v.string()),
  },
  returns: v.object({
    deduped: v.boolean(),
    cancelledTurn: v.optional(v.string()),
  }),
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
      ...(args.senderId === undefined ? {} : { senderId: args.senderId }),
      ...(args.kind === undefined ? {} : { kind: args.kind }),
    });

    // Signals (read receipts, typing) are recorded for visibility but never
    // become turn input — stored already-handled so no flush can drain them.
    const triggersTurn = args.triggersTurn ?? true;

    const rowId = await ctx.db.insert("inbound", {
      spaceId: args.spaceId,
      messageId: args.messageId,
      platform: args.platform,
      message: args.message,
      queuedAt: now,
      status: triggersTurn ? ("queued" as const) : ("handled" as const),
      ...(args.raw === undefined ? {} : { raw: args.raw }),
    });

    if (!triggersTurn) {
      // Critically, do NOT cancel the in-flight turn or re-arm the debounce.
      // A read receipt arriving mid-generation must not cancel the reply it is
      // acknowledging, and must not extend the window that reply is waiting on.
      return { deduped: false };
    }

    if ((args.mode ?? "immediate") === "immediate") {
      // Every message is its own turn. Nothing is superseded, so a reply
      // already in flight is left alone and no message waits on another.
      await scheduleFlush(ctx, {
        spaceId: args.spaceId,
        debounceMs: args.debounceMs ?? DEFAULT_DEBOUNCE_MS,
        ...(args.onTurn === undefined ? {} : { onTurn: args.onTurn }),
        only: rowId,
      });
      return { deduped: false };
    }

    // Collapse mode: a new message supersedes whatever turn is in flight —
    // cancel it, carry its messages forward, then restart the window.
    const cancelledTurn = await cancelCurrentTurn(ctx, args.spaceId);
    await scheduleFlush(ctx, {
      spaceId: args.spaceId,
      debounceMs: args.debounceMs ?? DEFAULT_DEBOUNCE_MS,
      ...(args.onTurn === undefined ? {} : { onTurn: args.onTurn }),
    });

    return {
      deduped: false,
      ...(cancelledTurn === undefined ? {} : { cancelledTurn }),
    };
  },
});

/** Look up a space's routing record (`senderId`, `kind`) for the send path. */
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
      ...(space.senderId === undefined ? {} : { senderId: space.senderId }),
      ...(space.kind === undefined ? {} : { kind: space.kind }),
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
