import type { Message } from "@photon-ai/api";
import { Webhook, WebhookVerificationError } from "standardwebhooks";

/**
 * Webhook verification and parsing. It runs in Convex's default runtime,
 * which has no Node builtins; `standardwebhooks` is plain JavaScript.
 */

/**
 * The webhook API versions this release reads. A destination's API version is
 * fixed when it is created, and every event carries it as `apiVersion`.
 */
const SUPPORTED_API_VERSIONS: ReadonlySet<string> = new Set(["2026-07-01"]);

/** One `message.received` event, flattened for storage. */
export interface InboundMessage {
  /** Space kind, e.g. `direct` or `group` (`space.kind`). */
  kind?: string;
  /** The event's `data.object`, stored as delivered. */
  message: Message;
  messageId: string;
  platform: string;
  /** The agent's own user in the space (`receivedBy.id`). */
  senderId?: string;
  spaceId: string;
  /**
   * False for read receipts and typing indicators. They are stored but start
   * no turn: a bot that answered the read receipt for its own reply would
   * echo forever.
   */
  triggersTurn: boolean;
}

export type ParseOutcome =
  | { ok: true; message: InboundMessage }
  | { ok: false; status: number; reason: string }
  // Acknowledged without storing: another event type, or a message with no
  // space. Nothing is wrong with the delivery, so it gets a 200.
  | { ok: false; status: 200; reason: string; acknowledged: true };

const SIGNATURE_INVALID_STATUS = 401;
const MALFORMED_STATUS = 400;
const SERVER_ERROR_STATUS = 500;
const OK_STATUS = 200;

/**
 * Content types that describe the conversation instead of contributing to it.
 * Kept as a denylist rather than an allowlist of "real" messages so that a
 * content type this release predates still reaches the handler.
 */
const SIGNAL_CONTENT_TYPES: ReadonlySet<string> = new Set([
  "message.read",
  "space.typing.update",
]);

type JsonObject = Record<string, unknown>;

const asObject = (value: unknown): JsonObject | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;

const asString = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

const malformed = (reason: string): ParseOutcome => ({
  ok: false,
  status: MALFORMED_STATUS,
  reason,
});

const acknowledged = (reason: string): ParseOutcome => ({
  ok: false,
  status: OK_STATUS,
  reason,
  acknowledged: true,
});

/**
 * Verify and parse one webhook delivery.
 *
 * Verification runs over the body text exactly as received and BEFORE any
 * interpretation, so a forged body is rejected without its contents ever being
 * read. Pass the headers as a plain object: `standardwebhooks` reads them with
 * `Object.keys`, which sees nothing on a Fetch `Headers`.
 */
export function parseWebhook(input: {
  body: string;
  headers: Record<string, string>;
  secret: string;
}): ParseOutcome {
  let webhook: Webhook;
  try {
    webhook = new Webhook(input.secret);
  } catch {
    // Never echo the secret, or the library's message about it.
    return {
      ok: false,
      status: SERVER_ERROR_STATUS,
      reason: "webhook secret cannot be decoded as a signing secret",
    };
  }

  let payload: unknown;
  try {
    payload = webhook.verify(input.body, input.headers);
  } catch (error) {
    if (error instanceof WebhookVerificationError) {
      return {
        ok: false,
        status: SIGNATURE_INVALID_STATUS,
        reason: error.message,
      };
    }
    if (error instanceof SyntaxError) {
      return malformed("body is not valid JSON");
    }
    throw error;
  }

  const event = asObject(payload);
  if (!event) {
    return malformed("body is not a JSON object");
  }
  const apiVersion = asString(event.apiVersion);
  if (!(apiVersion && SUPPORTED_API_VERSIONS.has(apiVersion))) {
    return malformed(
      `unsupported apiVersion ${JSON.stringify(event.apiVersion ?? null)}; this release reads ${[...SUPPORTED_API_VERSIONS].join(", ")}`
    );
  }
  const type = asString(event.type);
  if (!type) {
    return malformed("missing type");
  }
  // Checked before `data.object.id`: some event types carry no object id, or no
  // object at all, and they are still well-formed.
  if (type !== "message.received") {
    return acknowledged(`ignored event type ${type}`);
  }
  const object = asObject(asObject(event.data)?.object);
  const messageId = asString(object?.id);
  if (!(object && messageId)) {
    return malformed("missing data.object.id");
  }
  const platform = asString(object.platform);
  if (!platform) {
    return malformed("missing data.object.platform");
  }
  const space = asObject(object.space);
  const spaceId = asString(space?.id);
  if (!spaceId) {
    return acknowledged("unroutable message (no data.object.space.id)");
  }

  const senderId = asString(asObject(object.receivedBy)?.id);
  const kind = asString(space?.kind);
  const contentType = asString(asObject(object.content)?.type);
  return {
    ok: true,
    message: {
      spaceId,
      messageId,
      platform,
      message: object as Message,
      senderId,
      kind,
      triggersTurn: !(
        contentType !== undefined && SIGNAL_CONTENT_TYPES.has(contentType)
      ),
    },
  };
}
