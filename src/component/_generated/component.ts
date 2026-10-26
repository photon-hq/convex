/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    inbound: {
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          debounceMs?: number;
          kind?: string;
          message: any;
          messageId: string;
          mode?: "immediate" | "collapse";
          onTurn?: string;
          platform: string;
          raw?: any;
          senderId?: string;
          spaceId: string;
          triggersTurn?: boolean;
        },
        { cancelledTurn?: string; deduped: boolean },
        Name
      >;
      getSpace: FunctionReference<
        "query",
        "internal",
        { spaceId: string },
        null | {
          kind?: string;
          lastInboundAt: number;
          platform: string;
          senderId?: string;
          spaceId: string;
        },
        Name
      >;
      listMessages: FunctionReference<
        "query",
        "internal",
        { limit?: number; spaceId: string },
        Array<{
          message: any;
          messageId: string;
          queuedAt: number;
          status: string;
        }>,
        Name
      >;
    };
    outbox: {
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          apiKey?: string;
          baseUrl?: string;
          content?: any;
          email?: any;
          kind: "sendMessage" | "markMessageRead" | "setSpaceTyping";
          pacingMs?: number;
          parentMessageId?: string;
          projectId?: string;
          spaceId: string;
          targetMessageId?: string;
          telegram?: any;
          turnId?: string;
        },
        { idempotencyKey?: string; queued: boolean; reason?: string },
        Name
      >;
      list: FunctionReference<
        "query",
        "internal",
        { limit?: number; spaceId: string },
        Array<{
          attempts: number;
          idempotencyKey: string;
          kind: string;
          lastError?: string;
          messageId?: string;
          queuedAt: number;
          sentAt?: number;
          status: string;
        }>,
        Name
      >;
    };
    pipeline: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { spaceId: string },
        null | string,
        Name
      >;
      completeTurn: FunctionReference<
        "mutation",
        "internal",
        { spaceId: string; turnId: string },
        null,
        Name
      >;
      isCancelled: FunctionReference<
        "query",
        "internal",
        { spaceId: string; turnId: string },
        boolean,
        Name
      >;
    };
  };
