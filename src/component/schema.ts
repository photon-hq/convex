import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

/**
 * A Photon `Message`, stored exactly as the `message.received` event delivered
 * it (`data.object`). Kept as an opaque `v.any()` rather than a discriminated
 * union on purpose: the API adds fields and content types without a new API
 * version, and a strict validator here would reject a delivery that
 * `@photon-ai/api` itself is happy to read. Narrowing happens in app code,
 * where the `Message` type from `@photon-ai/api` applies.
 */
const message = v.any();

export const inboundStatus = v.union(
  // Waiting for a flush to pick it up.
  v.literal("queued"),
  // Handed to a batch; still owned by a live turn.
  v.literal("drained"),
  // The turn that drained it completed.
  v.literal("handled")
);

export const outboxStatus = v.union(
  v.literal("pending"),
  v.literal("sending"),
  v.literal("sent"),
  v.literal("failed"),
  // The turn that queued this send was superseded before it went out.
  v.literal("cancelled")
);

export default defineSchema({
  /**
   * Every verified inbound message, and the batch queue itself. Messages stay
   * here until a flush drains them — the "drain in the handler, not the
   * enqueuer" rule — so a cancelled flush loses nothing.
   */
  inbound: defineTable({
    spaceId: v.string(),
    // Photon message id (`pho_msg_…`). Webhook deliveries are at-least-once,
    // so this is the dedupe key.
    messageId: v.string(),
    platform: v.string(),
    message,
    // When the component stored it. Not `receivedAt`, which would read as the
    // message's own `timestamps.receivedAt`.
    queuedAt: v.number(),
    status: inboundStatus,
    // Set when a turn drains this row, so cancellation can find its messages.
    turnId: v.optional(v.string()),
    // The rest of the event, without `data.object` (stored above as `message`).
    raw: v.optional(v.any()),
  })
    .index("by_message", ["messageId"])
    .index("by_space_queued", ["spaceId", "queuedAt"])
    .index("by_space_status", ["spaceId", "status"])
    .index("by_turn", ["turnId"]),

  /**
   * Routing registry. Replies need the `senderId` — the agent's own user in
   * this space, from the event's `receivedBy` — which is not derivable from
   * the space id alone.
   */
  spaces: defineTable({
    spaceId: v.string(),
    platform: v.string(),
    senderId: v.optional(v.string()),
    kind: v.optional(v.string()),
    lastInboundAt: v.number(),
  }).index("by_space", ["spaceId"]),

  /**
   * Per-space record of the turn that currently owns the pipeline. A turn is
   * one conversational exchange: flush → handler → sends.
   *
   * `ctx.scheduler.cancel` cannot stop an action that has already started, so
   * cancellation is cooperative: a running handler polls `isCancelled`.
   *
   * Liveness is decided by turn *identity* (`turnId` / `cancelledTurnId`),
   * never by comparing timestamps. Two pipeline events can land in the same
   * millisecond — a message arriving as its own flush runs — and a
   * `cancelledAt > turnStartedAt` test would then read a cancelled turn as
   * live. `cancelledAt` is kept for audit only.
   */
  inflight: defineTable({
    spaceId: v.string(),
    turnId: v.optional(v.string()),
    turnStartedAt: v.optional(v.number()),
    /** The turn `cancel` last killed. Compared by identity, not by time. */
    cancelledTurnId: v.optional(v.string()),
    /** Audit only — never used to decide whether a turn is live. */
    cancelledAt: v.optional(v.number()),
    flushJobId: v.optional(v.id("_scheduled_functions")),
    handlerJobId: v.optional(v.id("_scheduled_functions")),
  }).index("by_space", ["spaceId"]),

  /**
   * One row per turn, and the authority on whether that turn is still live.
   *
   * `inflight` tracks only the *current* turn for a space, which is enough for
   * collapse mode but wrong for immediate mode, where several turns are open at
   * once and are independent. Deriving liveness from "is this the current
   * turn?" would let each new message silently invalidate its predecessors.
   */
  turns: defineTable({
    turnId: v.string(),
    spaceId: v.string(),
    status: v.union(v.literal("live"), v.literal("cancelled")),
    startedAt: v.number(),
  }).index("by_turn", ["turnId"]),

  /**
   * Messages a cancelled turn had already drained. The next flush prepends
   * them so the model still sees them — as history, not as fresh input.
   *
   * A flush *claims* these rows (stamps `turnId`) rather than deleting them,
   * and they are removed only once that turn completes. Deleting at read time
   * loses them if the claiming turn is itself superseded — the same
   * "drain in the handler, not the enqueuer" rule that governs `inbound`.
   */
  carried: defineTable({
    spaceId: v.string(),
    messageId: v.string(),
    message,
    carriedAt: v.number(),
    turnId: v.optional(v.string()),
  })
    .index("by_space", ["spaceId"])
    .index("by_turn", ["turnId"]),

  /**
   * Outbound actions. `idempotencyKey` is derived from `(turnId, seq)`;
   * `sendMessage` and `markMessageRead` send it as the request's
   * `idempotency-key` header, and retries reuse it (see the note in
   * `outbox.ts`).
   */
  outbox: defineTable({
    spaceId: v.string(),
    turnId: v.optional(v.string()),
    seq: v.number(),
    idempotencyKey: v.string(),
    kind: v.union(
      v.literal("sendMessage"),
      v.literal("markMessageRead"),
      v.literal("setSpaceTyping")
    ),
    content: v.optional(v.any()),
    parentMessageId: v.optional(v.string()),
    email: v.optional(v.any()),
    telegram: v.optional(v.any()),
    // `markMessageRead` only: the message to mark read.
    targetMessageId: v.optional(v.string()),
    status: outboxStatus,
    attempts: v.number(),
    queuedAt: v.number(),
    sentAt: v.optional(v.number()),
    messageId: v.optional(v.string()),
    lastError: v.optional(v.string()),
    jobId: v.optional(v.id("_scheduled_functions")),
  })
    .index("by_idempotency_key", ["idempotencyKey"])
    .index("by_space_queued", ["spaceId", "queuedAt"])
    .index("by_turn", ["turnId"]),

  /**
   * The Photon API credentials the send jobs use — a single row. The app's
   * `PhotonComponent` passes them with every send, and `outbox.enqueue` writes
   * them here only when they changed. Anyone with access to this deployment's
   * dashboard can read the API key.
   */
  config: defineTable({
    projectId: v.string(),
    apiKey: v.string(),
    baseUrl: v.optional(v.string()),
  }),
});
