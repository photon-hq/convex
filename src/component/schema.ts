import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

/**
 * A Photon `Message`, as the event delivered it (`data.object`). `v.any()`
 * because the API adds fields and content types within an API version; apps
 * narrow it with the `Message` type from `@photon-ai/api`.
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

export const sendKind = v.union(
  v.literal("sendMessage"),
  v.literal("markMessageRead"),
  v.literal("setSpaceTyping")
);

/**
 * One outbound action, as `outbox.enqueue` takes it, the `outbox` table keeps
 * it and `outbox.loadJob` hands it to `transport.execute`.
 */
export const sendFields = {
  spaceId: v.string(),
  kind: sendKind,
  content: v.optional(v.any()),
  parentMessageId: v.optional(v.string()),
  email: v.optional(v.any()),
  telegram: v.optional(v.any()),
  // `markMessageRead` only: the message to mark read.
  targetMessageId: v.optional(v.string()),
};

export const credentials = v.object({
  projectId: v.string(),
  apiKey: v.string(),
  baseUrl: v.optional(v.string()),
});

export default defineSchema({
  /**
   * Every verified inbound message, and the queue flushes drain. A message
   * stays queued until a flush drains it, so a cancelled flush loses nothing.
   */
  inbound: defineTable({
    spaceId: v.string(),
    // Photon message id (`pho_msg_…`). Webhook deliveries are at-least-once,
    // so this is the dedupe key.
    messageId: v.string(),
    message,
    // When the component stored it. Not `receivedAt`, which would read as the
    // message's own `timestamps.receivedAt`.
    queuedAt: v.number(),
    status: inboundStatus,
    // Set when a turn drains this row, so cancellation can find its messages.
    turnId: v.optional(v.id("turns")),
  })
    .index("by_messageId", ["messageId"])
    .index("by_spaceId_and_queuedAt", ["spaceId", "queuedAt"])
    .index("by_spaceId_and_status", ["spaceId", "status"])
    .index("by_turnId", ["turnId"]),

  /**
   * Routing registry. Replies are sent as `senderId`, the agent's own user in
   * the space, which only an event's `receivedBy` tells.
   */
  spaces: defineTable({
    spaceId: v.string(),
    platform: v.string(),
    senderId: v.optional(v.string()),
    kind: v.optional(v.string()),
  }).index("by_spaceId", ["spaceId"]),

  /** Each space's current collapse turn, and its flush and handler jobs. */
  inflight: defineTable({
    spaceId: v.string(),
    turnId: v.optional(v.id("turns")),
    flushJobId: v.optional(v.id("_scheduled_functions")),
    handlerJobId: v.optional(v.id("_scheduled_functions")),
  }).index("by_spaceId", ["spaceId"]),

  /**
   * One row per turn, whose id is the turn id, and the authority on whether it
   * is still live. Not `inflight`: in immediate mode several turns are open at
   * once, and "is this the space's current turn?" would let each new message
   * cancel its predecessors.
   */
  turns: defineTable({
    status: v.union(v.literal("live"), v.literal("cancelled")),
  }),

  /**
   * Messages a cancelled turn had drained, which the next turn gets as
   * `carried`. The flush that hands them on claims them (sets `turnId`). When
   * the next message supersedes that turn, they are deleted if it answered
   * and completed, and handed on otherwise.
   */
  carried: defineTable({
    spaceId: v.string(),
    messageId: v.string(),
    message,
    turnId: v.optional(v.id("turns")),
  })
    .index("by_spaceId", ["spaceId"])
    .index("by_turnId", ["turnId"]),

  /** Outbound actions, one row each. A row's id is its idempotency key. */
  outbox: defineTable({
    ...sendFields,
    turnId: v.optional(v.id("turns")),
    status: outboxStatus,
    attempts: v.number(),
    queuedAt: v.number(),
    sentAt: v.optional(v.number()),
    messageId: v.optional(v.string()),
    lastError: v.optional(v.string()),
    // Unset while a reply waits for the one queued before it to settle.
    jobId: v.optional(v.id("_scheduled_functions")),
    // A waiting reply's pause after that one settles.
    pacingMs: v.optional(v.number()),
  })
    .index("by_spaceId_and_queuedAt", ["spaceId", "queuedAt"])
    .index("by_turnId", ["turnId"]),

  /**
   * The Photon API credentials the send jobs use — a single row. The app's
   * `PhotonComponent` passes them with every send, and `outbox.enqueue` writes
   * them here only when they changed. Anyone with access to this deployment's
   * dashboard can read the API key.
   */
  config: defineTable(credentials),
});
