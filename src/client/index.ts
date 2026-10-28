import type {
  Message,
  SendContent,
  SendEmailOptions,
  SendTelegramOptions,
} from "@photon-ai/api";
import {
  createFunctionHandle,
  type FunctionReference,
  type FunctionReturnType,
  type GenericActionCtx,
  type GenericDataModel,
  type GenericMutationCtx,
  type GenericQueryCtx,
  type HttpRouter,
  httpActionGeneric,
} from "convex/server";
import type { ComponentApi } from "../component/_generated/component.js";
import { parseWebhook } from "../shared/wire.js";

/**
 * App-facing client for the Photon component.
 *
 * Wraps the component's functions for app code, and runs in the app's own
 * environment — which is what gives it access to the app's `PHOTON_*`
 * environment variables (a component cannot read the app's env vars).
 */

export const DEFAULT_WEBHOOK_PATH = "/photon/webhook";
const OK_STATUS = 200;
const SERVER_ERROR_STATUS = 500;

/** The turn a handler is invoked with — one conversational exchange. */
export interface Turn {
  /**
   * Messages a previous, cancelled turn had already drained. Present them as
   * earlier context rather than fresh input — the user has been waiting on
   * these longer than the ones in `messages`.
   */
  carried: Message[];
  /** Messages from this burst, oldest first. */
  messages: Message[];
  spaceId: string;
  turnId: string;
}

/** One stored inbound message, as `listMessages` returns it. */
export interface StoredMessage {
  message: Message;
  messageId: string;
  /** When the component stored it. */
  queuedAt: number;
  status: string;
}

/**
 * Narrow a turn reference down to exactly the fields the component's
 * validators accept.
 *
 * Handlers are naturally called as `photon.isCancelled(ctx, args)` where
 * `args` is the whole turn — including `messages` and `carried`. Convex
 * rejects extra fields, so passing that through verbatim throws
 * `ArgumentValidationError`. Narrowing here keeps the ergonomic call working
 * instead of pushing the destructuring onto every caller.
 */
export const pickTurn = (ref: {
  spaceId: string;
  turnId: string;
}): { spaceId: string; turnId: string } => ({
  spaceId: ref.spaceId,
  turnId: ref.turnId,
});

// Convenient types for `ctx` args, that only include the bare minimum.
type QueryCtx = Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
type MutationCtx = Pick<
  GenericMutationCtx<GenericDataModel>,
  "runQuery" | "runMutation"
>;
type ActionCtx = Pick<
  GenericActionCtx<GenericDataModel>,
  "runQuery" | "runMutation" | "runAction"
>;

/**
 * How inbound messages become turns.
 *
 * - `"immediate"` (default) — every message is handled on its own. No batching,
 *   no waiting, no supersession. Simple and predictable: N messages produce N
 *   handler calls. `turn.messages` always has exactly one entry and
 *   `turn.carried` is always empty.
 *
 * - `"collapse"` — a burst becomes one turn. A new message supersedes the
 *   in-flight turn, cancels any reply that has not shipped, and carries its
 *   messages into the next one as prior context. Use when replies are expensive
 *   (an LLM call) or when answering each message separately would read as the
 *   agent talking over itself.
 */
export type TurnMode = "immediate" | "collapse";

export interface PhotonComponentOptions {
  /** Photon project API key. Falls back to `process.env.PHOTON_API_KEY`. */
  apiKey?: string;
  /**
   * Photon API origin. Falls back to `process.env.PHOTON_BASE_URL`, then to
   * the `@photon-ai/api` default, `https://api.photon.codes`.
   */
  baseUrl?: string;
  /**
   * Settling window before a turn is handled. Only meaningful in `"collapse"`
   * mode, where it widens the window in which a follow-up can join the same
   * turn. It is a latency cost on every reply, so leave it at 0 unless you
   * specifically need tighter grouping — cancellation already collapses most
   * bursts on its own.
   *
   * @default 0
   */
  debounceMs?: number;
  /**
   * How inbound messages become turns. See {@link TurnMode}.
   *
   * @default "immediate"
   */
  mode?: TurnMode;
  /**
   * The app function invoked once per turn. Usually an `internalAction` so it
   * can call an LLM.
   */
  onTurn?: FunctionReference<"action" | "mutation", "internal" | "public">;
  /**
   * Gap between a turn's replies. They go out one at a time, in the order they
   * were queued, each after the first at least `pacingMs` after the one before
   * it. Read receipts and reactions are never held back or delayed.
   */
  pacingMs?: number;
  /** Photon project id (`pho_prj_…`). Falls back to `process.env.PHOTON_PROJECT_ID`. */
  projectId?: string;
  /** @default "/photon/webhook" */
  webhookPath?: string;
  /**
   * Signing secret (`whsec_…`) of the Photon webhook destination. Falls back
   * to `process.env.PHOTON_WEBHOOK_SECRET`.
   */
  webhookSecret?: string;
}

/** The outbox's answer to a queued action. */
type Queued = FunctionReturnType<ComponentApi["outbox"]["enqueue"]>;

export class PhotonComponent {
  readonly component: ComponentApi;
  readonly options: PhotonComponentOptions;
  private readonly projectId: string | undefined;
  private readonly apiKey: string | undefined;
  private readonly baseUrl: string | undefined;

  constructor(component: ComponentApi, options: PhotonComponentOptions = {}) {
    this.component = component;
    this.options = options;
    this.projectId = options.projectId ?? process.env.PHOTON_PROJECT_ID;
    this.apiKey = options.apiKey ?? process.env.PHOTON_API_KEY;
    this.baseUrl = options.baseUrl ?? process.env.PHOTON_BASE_URL;
  }

  private secret(): string | undefined {
    return this.options.webhookSecret ?? process.env.PHOTON_WEBHOOK_SECRET;
  }

  private async handleRef(): Promise<string | undefined> {
    return this.options.onTurn
      ? await createFunctionHandle(this.options.onTurn)
      : undefined;
  }

  /**
   * The credentials every send carries to the component, which keeps the
   * latest in its `config` table for the send jobs.
   */
  private credentials(): {
    projectId: string;
    apiKey: string;
    baseUrl?: string;
  } {
    if (!(this.projectId && this.apiKey)) {
      throw new Error(
        "Photon credentials missing: set PHOTON_PROJECT_ID and PHOTON_API_KEY, or pass projectId and apiKey to PhotonComponent"
      );
    }
    return {
      projectId: this.projectId,
      apiKey: this.apiKey,
      ...(this.baseUrl === undefined ? {} : { baseUrl: this.baseUrl }),
    };
  }

  /**
   * Mount the webhook endpoint.
   *
   * The handler verifies and persists the delivery *before* answering 200, so
   * an acknowledged message is already durable. It deliberately does no work
   * after the response — on Convex the isolate is frozen once the response is
   * returned.
   */
  registerRoutes(http: HttpRouter, opts: { path?: string } = {}): void {
    const path = opts.path ?? this.options.webhookPath ?? DEFAULT_WEBHOOK_PATH;

    http.route({
      path,
      method: "POST",
      handler: httpActionGeneric(async (ctx, request) => {
        const secret = this.secret();
        if (!secret) {
          return new Response(
            "webhook secret not configured (set PHOTON_WEBHOOK_SECRET)",
            { status: SERVER_ERROR_STATUS }
          );
        }

        const outcome = parseWebhook({
          body: await request.text(),
          headers: Object.fromEntries(request.headers),
          secret,
        });

        if (!outcome.ok) {
          return new Response(outcome.reason, { status: outcome.status });
        }

        const { message } = outcome;
        const onTurn = await this.handleRef();
        await ctx.runMutation(this.component.inbound.enqueue, {
          spaceId: message.spaceId,
          messageId: message.messageId,
          platform: message.platform,
          message: message.message,
          ...(message.senderId === undefined
            ? {}
            : { senderId: message.senderId }),
          ...(message.kind === undefined ? {} : { kind: message.kind }),
          triggersTurn: message.triggersTurn,
          mode: this.options.mode ?? "immediate",
          raw: message.raw,
          ...(this.options.debounceMs === undefined
            ? {}
            : { debounceMs: this.options.debounceMs }),
          ...(onTurn === undefined ? {} : { onTurn }),
        });

        return new Response("ok", { status: OK_STATUS });
      }),
    });
  }

  /**
   * Has this turn been superseded by a newer message?
   *
   * `ctx.scheduler.cancel` can't stop an action that is already running, so a
   * handler doing slow work (generating, sending) should poll this between
   * steps and return early when it flips.
   */
  async isCancelled(
    ctx: QueryCtx | MutationCtx | ActionCtx,
    args: { spaceId: string; turnId: string }
  ): Promise<boolean> {
    return await ctx.runQuery(
      this.component.pipeline.isCancelled,
      pickTurn(args)
    );
  }

  /**
   * Queue an outbound message. Rejected if the turn was superseded. With
   * `parentMessageId` it is a reply to that message.
   */
  async sendMessage(
    ctx: MutationCtx | ActionCtx,
    args: {
      spaceId: string;
      content: SendContent;
      turnId?: string;
      parentMessageId?: string;
      /** Email only: subject and recipients. */
      email?: SendEmailOptions;
      /** Telegram only. */
      telegram?: SendTelegramOptions;
      pacingMs?: number;
    }
  ): Promise<Queued> {
    return await ctx.runMutation(this.component.outbox.enqueue, {
      spaceId: args.spaceId,
      kind: "sendMessage" as const,
      content: args.content,
      ...(args.turnId === undefined ? {} : { turnId: args.turnId }),
      ...(args.parentMessageId === undefined
        ? {}
        : { parentMessageId: args.parentMessageId }),
      ...(args.email === undefined ? {} : { email: args.email }),
      ...(args.telegram === undefined ? {} : { telegram: args.telegram }),
      pacingMs: args.pacingMs ?? this.options.pacingMs ?? 0,
      ...this.credentials(),
    });
  }

  /** Mark the newest message of a turn read. */
  async markMessageRead(
    ctx: MutationCtx | ActionCtx,
    args: { spaceId: string; turnId: string }
  ): Promise<Queued> {
    return await ctx.runMutation(this.component.outbox.enqueue, {
      spaceId: args.spaceId,
      kind: "markMessageRead" as const,
      turnId: args.turnId,
      ...this.credentials(),
    });
  }

  /** Mark a turn's messages handled. Call when the handler finishes. */
  async completeTurn(
    ctx: MutationCtx | ActionCtx,
    args: { spaceId: string; turnId: string }
  ): Promise<void> {
    await ctx.runMutation(this.component.pipeline.completeTurn, pickTurn(args));
  }

  /** Routing record for a space (`senderId`, `kind`), or `null`. */
  async getSpace(
    ctx: QueryCtx | MutationCtx | ActionCtx,
    args: { spaceId: string }
  ): Promise<FunctionReturnType<ComponentApi["inbound"]["getSpace"]>> {
    return await ctx.runQuery(this.component.inbound.getSpace, args);
  }

  /** Recent inbound messages for a space, oldest first. */
  async listMessages(
    ctx: QueryCtx | MutationCtx | ActionCtx,
    args: { spaceId: string; limit?: number }
  ): Promise<StoredMessage[]> {
    return await ctx.runQuery(this.component.inbound.listMessages, args);
  }

  /**
   * Recent outbox rows for a space, oldest first — delivery status and
   * failure audit trail.
   */
  async listOutbox(
    ctx: QueryCtx | MutationCtx | ActionCtx,
    args: { spaceId: string; limit?: number }
  ): Promise<FunctionReturnType<ComponentApi["outbox"]["list"]>> {
    return await ctx.runQuery(this.component.outbox.list, args);
  }
}

export type { ComponentApi } from "../component/_generated/component.js";
export { parseWebhook } from "../shared/wire.js";
