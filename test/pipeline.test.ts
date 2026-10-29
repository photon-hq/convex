import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, internal } from "../src/component/_generated/api.js";
import type { Id } from "../src/component/_generated/dataModel.js";
import { cancelCurrentTurn } from "../src/component/pipeline.js";
import schema from "../src/component/schema.js";

/**
 * Pipeline behavior tests, run against the component's own schema in
 * `convex-test`'s in-memory backend.
 *
 * These target the rules that are easy to get subtly wrong and expensive to
 * discover in production: at-least-once dedupe, the debounce reset, the
 * carry-forward rule, and the per-turn (not per-space) cancellation check.
 */

// `import.meta.glob` is how convex-test discovers the function modules. The
// component's files live outside a conventional `convex/` dir, so the glob is
// pointed at them explicitly.
const modules = import.meta.glob("../src/component/**/*.ts");

const setup = () => convexTest(schema, modules);

// The pipeline is scheduler-driven: the debounce window is a real
// `runAfter`. Fake timers let a test jump the window instead of sleeping, and
// `finishAllScheduledFunctions` needs the timer-advancing callback to actually
// fire the jobs it is waiting on.
beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

/** Supersede the space's current turn, as a follow-up message does. */
const cancelTurn = async (t: ReturnType<typeof setup>): Promise<void> => {
  await t.run(async (ctx) => {
    await cancelCurrentTurn(ctx, "chat-1");
  });
};

/** Run every scheduled job, advancing the debounce timers to fire them. */
const settle = async (t: ReturnType<typeof setup>): Promise<void> => {
  await t.finishAllScheduledFunctions(vi.runAllTimers);
};

/**
 * A message in `collapse` mode. The suites below exercise supersession,
 * carry-forward and batching, which only exist in that mode — the component
 * default is `immediate` (one message, one turn), covered separately at the
 * bottom of this file.
 */
const message = (overrides: Record<string, unknown> = {}) => ({
  spaceId: "chat-1",
  messageId: "m1",
  platform: "imessage",
  message: { content: { type: "text", format: "plain", text: "hey" } },
  mode: "collapse" as const,
  ...overrides,
});

describe("inbound.enqueue", () => {
  it("stores a verified message and registers the space", async () => {
    const t = setup();
    const result = await t.mutation(api.inbound.enqueue, message());
    expect(result.deduped).toBe(false);

    const space = await t.query(api.inbound.getSpace, { spaceId: "chat-1" });
    expect(space?.platform).toBe("imessage");

    const stored = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(stored).toHaveLength(1);
    expect(stored[0]?.messageId).toBe("m1");
    expect(stored[0]?.status).toBe("queued");
  });

  it("dedupes a redelivered message id without a second row", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    const second = await t.mutation(api.inbound.enqueue, message());

    expect(second.deduped).toBe(true);
    const stored = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(stored).toHaveLength(1);
  });

  it("reports the time of the space's newest message as lastInboundAt", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    // Move the clock without firing the first message's flush, which the
    // second message would otherwise cancel while it runs.
    vi.setSystemTime(Date.now() + 1000);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    const stored = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(stored[1]?.queuedAt).toBeGreaterThan(stored[0]?.queuedAt ?? 0);
    const space = await t.query(api.inbound.getSpace, { spaceId: "chat-1" });
    expect(space?.lastInboundAt).toBe(stored[1]?.queuedAt);
  });

  it("keeps a learned senderId when a later delivery omits it", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ senderId: "agent-1" }));
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    const space = await t.query(api.inbound.getSpace, { spaceId: "chat-1" });
    expect(space?.senderId).toBe("agent-1");
  });
});

describe("debounce and flush", () => {
  it("handles a burst as one batch after the window settles", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await t.mutation(
      api.inbound.enqueue,
      message({ messageId: "m3", debounceMs: 5000 })
    );

    // Nothing has been drained yet — the timer is still running.
    let stored = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(stored.every((m) => m.status === "queued")).toBe(true);

    await settle(t);

    stored = await t.query(api.inbound.listMessages, { spaceId: "chat-1" });
    expect(stored).toHaveLength(3);
    // One flush drained the whole burst into a single turn.
    expect(stored.every((m) => m.status === "drained")).toBe(true);
  });
});

describe("cancellation and carry-forward", () => {
  it("carries drained messages forward when a turn is superseded", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);

    // A new message lands mid-turn: the turn is cancelled and its drained
    // rows must not be lost.
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(1);
    expect(carried[0]?.messageId).toBe("m1");
  });

  it("carries a message once when two follow-ups land before the next flush", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);

    // m3 is enqueued before the flush m2 scheduled has run, so the turn m2
    // cancelled must not be cancelled, and its messages carried, again.
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await t.mutation(api.inbound.enqueue, message({ messageId: "m3" }));

    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried.map((row) => row.messageId)).toEqual(["m1"]);
  });

  it("claims carried messages on flush rather than deleting them", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await settle(t);

    // The consuming turn claims the row; it survives until that turn answers,
    // so a second supersession can still hand it on.
    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(1);
    expect(carried[0]?.turnId).toBeDefined();
  });

  it("keeps carried messages until a reply actually ships", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await settle(t);

    const turnId = (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;

    // Handler finished — but its reply is only queued, and a supersession can
    // still cancel it, so the inherited message must survive.
    await t.mutation(api.pipeline.completeTurn, {
      spaceId: "chat-1",
      turnId,
    });
    let carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(1);

    const queued = await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "reply" },
    });
    await t.mutation(internal.outbox.settle, {
      rowId: queued.idempotencyKey as Id<"outbox">,
      ok: true,
    });

    // The reply reached the user, so the turn's messages are finally spent.
    carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(0);
  });

  it("keeps every message across back-to-back supersessions", async () => {
    // The regression this replaced: a turn that inherited a carried message
    // and was then itself superseded dropped that message, because the flush
    // had already deleted it from the carry table. A user typing three
    // messages quickly lost the first one.
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m3" }));
    await settle(t);

    const seen = await t.run(async (ctx) => {
      const carried = await ctx.db.query("carried").collect();
      const inbound = await ctx.db.query("inbound").collect();
      const live = (await ctx.db.query("inflight").first())?.turnId;
      return [
        ...carried.map((c) => c.messageId),
        ...inbound
          .filter((row) => row.turnId === live && row.status === "drained")
          .map((row) => row.messageId),
      ];
    });

    expect(new Set(seen)).toEqual(new Set(["m1", "m2", "m3"]));
  });

  it("reports a superseded turn as cancelled, and a live one as not", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);

    const turnId = await t.run(async (ctx) => {
      const inflight = await ctx.db.query("inflight").first();
      return inflight?.turnId;
    });
    expect(turnId).toBeDefined();

    expect(
      await t.query(api.pipeline.isCancelled, {
        turnId: turnId as Id<"turns">,
      })
    ).toBe(false);

    await cancelTurn(t);

    expect(
      await t.query(api.pipeline.isCancelled, {
        turnId: turnId as Id<"turns">,
      })
    ).toBe(true);
  });

  it("treats an unknown turn as cancelled", async () => {
    const t = setup();
    const turnId = await t.run(async (ctx) => {
      const id = await ctx.db.insert("turns", { status: "live" as const });
      await ctx.db.delete("turns", id);
      return id;
    });
    expect(await t.query(api.pipeline.isCancelled, { turnId })).toBe(true);
  });
});

describe("outbox", () => {
  it("keys each send by its own outbox row", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const inflight = await ctx.db.query("inflight").first();
      return inflight?.turnId;
    })) as Id<"turns">;

    const first = await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "one" },
    });
    const second = await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "two" },
    });

    const rows = await t.run(
      async (ctx) => await ctx.db.query("outbox").collect()
    );
    expect(rows.map((row) => row._id)).toEqual([
      first.idempotencyKey,
      second.idempotencyKey,
    ]);
  });

  it("keys sends without a turn apart within one millisecond", async () => {
    const t = setup();
    const send = async (text: string) =>
      await t.mutation(api.outbox.enqueue, {
        spaceId: "chat-1",
        kind: "sendMessage" as const,
        content: { type: "text", format: "plain", text },
      });

    // Fake timers hold `Date.now()` still, as a single mutation does.
    const first = await send("one");
    const second = await send("two");

    expect(first.queued).toBe(true);
    expect(second.queued).toBe(true);
    expect(second.idempotencyKey).not.toBe(first.idempotencyKey);
  });

  it("refuses to queue a send from a superseded turn", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const inflight = await ctx.db.query("inflight").first();
      return inflight?.turnId;
    })) as Id<"turns">;

    await cancelTurn(t);

    const result = await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "too late" },
    });
    expect(result.queued).toBe(false);
    expect(result.reason).toBe("turn-cancelled");
  });

  it("cancels pending sends belonging to a superseded turn", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const inflight = await ctx.db.query("inflight").first();
      return inflight?.turnId;
    })) as Id<"turns">;

    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "queued" },
    });
    await cancelTurn(t);

    const rows = await t.query(api.outbox.list, { spaceId: "chat-1" });
    expect(rows).toHaveLength(1);
    expect(rows[0]?.status).toBe("cancelled");
  });

  it("allows a turnless send regardless of conversational state", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    await cancelTurn(t);

    const result = await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "proactive" },
    });
    expect(result.queued).toBe(true);
  });
});

describe("listing", () => {
  // Each test stores rows whose statuses are out of time order. Read by status
  // first, they would come back grouped by status, and a limit would keep the
  // wrong ones.

  it("returns a space's newest messages, oldest first, whatever their status", async () => {
    const t = setup();
    const statuses = ["handled", "queued", "drained", "handled"] as const;
    await t.run(async (ctx) => {
      for (const [i, status] of statuses.entries()) {
        await ctx.db.insert("inbound", {
          spaceId: "chat-1",
          messageId: `m${i + 1}`,
          message: {},
          queuedAt: i + 1,
          status,
        });
      }
    });
    const ids = async (limit?: number) =>
      (
        await t.query(api.inbound.listMessages, {
          spaceId: "chat-1",
          ...(limit === undefined ? {} : { limit }),
        })
      ).map((row) => row.messageId);

    expect(await ids()).toEqual(["m1", "m2", "m3", "m4"]);
    expect(await ids(2)).toEqual(["m3", "m4"]);
  });

  it("returns a space's newest outbox rows, oldest first, whatever their status", async () => {
    const t = setup();
    const statuses = ["sent", "failed", "sent", "pending"] as const;
    const ids = await t.run(async (ctx) => {
      const inserted: string[] = [];
      for (const [i, status] of statuses.entries()) {
        inserted.push(
          await ctx.db.insert("outbox", {
            spaceId: "chat-1",
            kind: "sendMessage",
            status,
            attempts: 1,
            queuedAt: i + 1,
          })
        );
      }
      return inserted;
    });
    const keys = async (limit?: number) =>
      (
        await t.query(api.outbox.list, {
          spaceId: "chat-1",
          ...(limit === undefined ? {} : { limit }),
        })
      ).map((row) => row.idempotencyKey);

    expect(await keys()).toEqual(ids);
    expect(await keys(2)).toEqual(ids.slice(2));
  });
});

describe("signals do not drive turns", () => {
  const readReceipt = (id: string) =>
    message({
      messageId: id,
      triggersTurn: false,
      message: {
        content: { type: "message.read" },
        parentMessageId: "outbound-1",
      },
    });

  it("never feeds a read receipt into a batch", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, readReceipt("r1"));
    await settle(t);

    // Stored for visibility, but already handled — no flush can drain it.
    const rows = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]?.status).toBe("handled");
  });

  it("does not arm the debounce, so a receipt alone starts no turn", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, readReceipt("r1"));
    await settle(t);

    const inflight = await t.run(
      async (ctx) => await ctx.db.query("inflight").first()
    );
    // No turn was ever created. This is what stops the echo feedback loop:
    // reply → read receipt → reply → … forever.
    expect(inflight?.turnId).toBeUndefined();
  });

  it("does not cancel the turn it is acknowledging", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;

    // A receipt for the bot's own reply lands while the turn is still open.
    await t.mutation(api.inbound.enqueue, readReceipt("r1"));

    expect(await t.query(api.pipeline.isCancelled, { turnId })).toBe(false);
  });
});

describe("pacing", () => {
  const turnFor = async (t: ReturnType<typeof setup>) => {
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    return (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;
  };

  it("does not delay the first reply of a turn", async () => {
    const t = setup();
    const turnId = await turnFor(t);

    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "first" },
      pacingMs: 800,
    });

    // Pacing separates consecutive messages; delaying the first is pure
    // latency before the user sees anything.
    const scheduled = await t.run(
      async (ctx) => await ctx.db.system.query("_scheduled_functions").collect()
    );
    const delivery = scheduled.find((j) => j.name.includes("claim"));
    expect(delivery).toBeDefined();
    expect(delivery?.scheduledTime).toBeLessThanOrEqual(
      (delivery?._creationTime ?? 0) + 1
    );
  });
});

describe("pacing counts messages, not control actions", () => {
  it("does not let an auto-markMessageRead push the reply into a paced slot", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;

    // The shape a real handler produces: acknowledge, then reply.
    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "markMessageRead" as const,
      pacingMs: 600,
    });
    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "first reply" },
      pacingMs: 600,
    });

    // markMessageRead occupies seq 0, but the reply is still the first *message* of
    // the turn and must not be delayed.
    const jobs = await t.run(
      async (ctx) => await ctx.db.system.query("_scheduled_functions").collect()
    );
    const deliveries = jobs.filter((j) => j.name.includes("claim"));
    expect(deliveries).toHaveLength(2);
    for (const job of deliveries) {
      expect(job.scheduledTime - job._creationTime).toBeLessThanOrEqual(1);
    }
  });
});

describe("no debounce: cancellation collapses bursts instead of waiting", () => {
  it("cancels a reply that has not gone out when a follow-up arrives", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    const firstTurn = (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;

    // The handler queued a reply; it has not gone out yet (a real send
    // takes a moment to go out).
    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId: firstTurn,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "reply to m1 only" },
    });

    // The follow-up lands inside that window.
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    const rows = await t.query(api.outbox.list, { spaceId: "chat-1" });
    // The stale reply never ships — this is what replaces the debounce.
    expect(rows.at(-1)?.status).toBe("cancelled");
  });

  it("hands the superseded message to the next turn as context", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    // m1 was already drained by the cancelled turn, so it moves to `carried`
    // and the next flush sees both — no message is answered twice or lost.
    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried.map((c) => c.messageId)).toEqual(["m1"]);
  });

  it("flushes immediately by default", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message());

    const jobs = await t.run(
      async (ctx) => await ctx.db.system.query("_scheduled_functions").collect()
    );
    const flush = jobs.find((j) => j.name.includes("flush"));
    expect(flush).toBeDefined();
    // `_creationTime` carries sub-millisecond precision, so compare with a
    // tolerance rather than to exactly 0.
    expect(
      Math.abs((flush?.scheduledTime ?? 0) - (flush?._creationTime ?? 0))
    ).toBeLessThan(1);
  });
});

describe("a message is answered only when its reply actually ships", () => {
  const queueReply = async (
    t: ReturnType<typeof setup>,
    turnId: Id<"turns">,
    text: string
  ) =>
    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text },
    });

  const liveTurn = async (t: ReturnType<typeof setup>) =>
    (await t.run(async (ctx) => {
      const row = await ctx.db.query("inflight").first();
      return row?.turnId;
    })) as Id<"turns">;

  it("carries a message forward when its queued reply gets cancelled", async () => {
    // `completeTurn` runs when the handler finishes, but the reply is only
    // *queued* then. A supersession moments later cancels it — so the message
    // must not stay marked as answered.
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    const turnId = await liveTurn(t);

    await queueReply(t, turnId, "echo: m1");
    await t.mutation(api.pipeline.completeTurn, {
      spaceId: "chat-1",
      turnId,
    });

    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried.map((c) => c.messageId)).toEqual(["m1"]);
  });

  it("does not re-answer a message whose reply already went out", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    const turnId = await liveTurn(t);

    const queued = await queueReply(t, turnId, "echo: m1");
    // The reply reached the user.
    await t.run(async (ctx) => {
      await ctx.db.patch("outbox", queued.idempotencyKey as Id<"outbox">, {
        status: "sent",
        sentAt: Date.now(),
      });
    });
    await t.mutation(api.pipeline.completeTurn, {
      spaceId: "chat-1",
      turnId,
    });

    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));

    // Genuinely answered, so it must not reappear as context.
    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(0);
  });

  /** m1, then m2 superseding its turn: the live turn answers m2 and inherits m1. */
  const inheritingTurn = async (t: ReturnType<typeof setup>) => {
    await t.mutation(api.inbound.enqueue, message({ messageId: "m1" }));
    await settle(t);
    await t.mutation(api.inbound.enqueue, message({ messageId: "m2" }));
    await settle(t);
    return await liveTurn(t);
  };

  /** Queue a reply for the turn and mark its request as going out. */
  const replyInFlight = async (
    t: ReturnType<typeof setup>,
    turnId: Id<"turns">
  ) => {
    const queued = await queueReply(t, turnId, "echo: m1 and m2");
    await t.run(async (ctx) => {
      await ctx.db.patch("outbox", queued.idempotencyKey as Id<"outbox">, {
        status: "sending",
        attempts: 1,
      });
    });
  };

  it("spends an inherited message with the completed turn whose reply is going out", async () => {
    const t = setup();
    const turnId = await inheritingTurn(t);
    await replyInFlight(t, turnId);
    await t.mutation(api.pipeline.completeTurn, {
      spaceId: "chat-1",
      turnId,
    });

    await t.mutation(api.inbound.enqueue, message({ messageId: "m3" }));
    await settle(t);

    // The reply going out answers m1, so m3's turn does not inherit it.
    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toEqual([]);
  });

  it("hands an inherited message on when the turn that answered it never completed", async () => {
    const t = setup();
    const turnId = await inheritingTurn(t);
    await replyInFlight(t, turnId);
    // No completeTurn: the handler was still running when m3 arrived.

    await t.mutation(api.inbound.enqueue, message({ messageId: "m3" }));
    await settle(t);

    const nextTurn = await liveTurn(t);
    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried.map((row) => [row.messageId, row.turnId])).toEqual([
      ["m1", nextTurn],
      ["m2", nextTurn],
    ]);
  });
});

describe("immediate mode (the default): one message, one turn", () => {
  // No `mode` — this is what a user gets out of the box.
  const plain = (overrides: Record<string, unknown> = {}) => ({
    spaceId: "chat-1",
    messageId: "m1",
    platform: "imessage",
    message: { content: { type: "text", format: "plain", text: "hey" } },
    ...overrides,
  });

  it("gives each message its own turn", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m1" }));
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m2" }));
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m3" }));
    await settle(t);

    const rows = await t.query(api.inbound.listMessages, {
      spaceId: "chat-1",
    });
    expect(rows).toHaveLength(3);
    // Three separate turns, not one shared batch.
    const turns = await t.run(async (ctx) => {
      const inbound = await ctx.db.query("inbound").collect();
      // Returned as an array: `t.run` can only hand back Convex values.
      return inbound.map((row) => row.turnId ?? "");
    });
    expect(new Set(turns).size).toBe(3);
  });

  it("never batches or carries messages", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m1" }));
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m2" }));
    await settle(t);

    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried).toHaveLength(0);
  });

  it("leaves an in-flight reply alone when the next message arrives", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m1" }));
    await settle(t);
    const turnId = (await t.run(async (ctx) => {
      const row = await ctx.db
        .query("inbound")
        .withIndex("by_messageId", (q) => q.eq("messageId", "m1"))
        .unique();
      return row?.turnId;
    })) as Id<"turns">;

    await t.mutation(api.outbox.enqueue, {
      spaceId: "chat-1",
      turnId,
      kind: "sendMessage" as const,
      content: { type: "text", format: "plain", text: "reply to m1" },
    });
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m2" }));

    // Nothing is superseded, so the first reply still ships.
    const rows = await t.query(api.outbox.list, { spaceId: "chat-1" });
    expect(rows.every((r) => r.status !== "cancelled")).toBe(true);
  });

  it("still dedupes redeliveries", async () => {
    const t = setup();
    await t.mutation(api.inbound.enqueue, plain({ messageId: "m1" }));
    const again = await t.mutation(
      api.inbound.enqueue,
      plain({ messageId: "m1" })
    );
    expect(again.deduped).toBe(true);
  });
});

describe("concurrent immediate turns stay independent", () => {
  const plain = (overrides: Record<string, unknown> = {}) => ({
    spaceId: "chat-1",
    messageId: "m1",
    platform: "imessage",
    message: { content: { type: "text", format: "plain", text: "hey" } },
    ...overrides,
  });

  it("does not let a later message invalidate an earlier turn's reply", async () => {
    // Regression: every turn used to share one `inflight` row, so each new
    // message overwrote `turnId` and silently cancelled its predecessors'
    // queued sends. In immediate mode all three replies must survive.
    const t = setup();
    const turnOf = async (messageId: string) =>
      (await t.run(async (ctx) => {
        const row = await ctx.db
          .query("inbound")
          .withIndex("by_messageId", (q) => q.eq("messageId", messageId))
          .unique();
        return row?.turnId;
      })) as Id<"turns">;

    for (const id of ["m1", "m2", "m3"]) {
      await t.mutation(api.inbound.enqueue, plain({ messageId: id }));
      await settle(t);
    }

    for (const id of ["m1", "m2", "m3"]) {
      const turnId = await turnOf(id);
      expect(await t.query(api.pipeline.isCancelled, { turnId })).toBe(false);

      const queued = await t.mutation(api.outbox.enqueue, {
        spaceId: "chat-1",
        turnId,
        kind: "sendMessage" as const,
        content: { type: "text", format: "plain", text: `echo: ${id}` },
      });
      expect(queued.queued).toBe(true);
    }

    const rows = await t.query(api.outbox.list, { spaceId: "chat-1" });
    expect(rows.filter((r) => r.status === "cancelled")).toHaveLength(0);
  });
});
