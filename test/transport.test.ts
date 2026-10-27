import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PhotonComponent } from "../src/client/index.js";
import { api } from "../src/component/_generated/api.js";
import type { ComponentApi } from "../src/component/_generated/component.js";
import schema from "../src/component/schema.js";
import {
  AGENT_ID,
  API_KEY,
  dispatched,
  failed,
  fakeFetch,
  json,
  PROJECT_ID,
  receivedMessage,
  SPACE_ID,
} from "./fixtures.js";

/**
 * The send path: `outbox.enqueue` → `claim` → `transport.execute` →
 * `settle`, against a stubbed `fetch`. `@photon-ai/api` reads
 * `globalThis.fetch` when a client is created, and `transport.execute`
 * creates one per run, so stubbing before the run is enough.
 */

const modules = import.meta.glob("../src/component/**/*.ts");
const setup = () => convexTest(schema, modules);

const BASE_URL = "https://api.example.invalid";
const credentials = {
  projectId: PROJECT_ID,
  apiKey: API_KEY,
  baseUrl: BASE_URL,
};
const MESSAGE_ID = "pho_msg_00000000000000000000000001";
const MISSING_CREDENTIALS = /PHOTON_PROJECT_ID and PHOTON_API_KEY/;
const text = { type: "text", format: "plain", text: "echo: Hello" } as const;

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

const settle = async (t: ReturnType<typeof setup>): Promise<void> => {
  await t.finishAllScheduledFunctions(vi.runAllTimers);
};

/** Stub the Photon API with scripted responses. */
const stubApi = (...responses: (() => Response)[]) => {
  const stub = fakeFetch(...responses);
  vi.stubGlobal("fetch", stub.fetch);
  return stub;
};

/** Store one received message and start its turn; returns the turn id. */
const startTurn = async (
  t: ReturnType<typeof setup>,
  overrides: Record<string, unknown> = {}
): Promise<string> => {
  await t.mutation(api.inbound.enqueue, {
    spaceId: SPACE_ID,
    messageId: MESSAGE_ID,
    platform: "imessage",
    message: receivedMessage(),
    senderId: AGENT_ID,
    kind: "direct",
    ...overrides,
  });
  await settle(t);
  return (await t.run(async (ctx) => {
    const row = await ctx.db
      .query("inbound")
      .withIndex("by_message", (q) =>
        q.eq("messageId", String(overrides.messageId ?? MESSAGE_ID))
      )
      .unique();
    return row?.turnId;
  })) as string;
};

const outbox = async (t: ReturnType<typeof setup>) =>
  await t.query(api.outbox.list, { spaceId: SPACE_ID });

describe("requests", () => {
  it.each([
    ["a plain message", {}, {}],
    [
      "a reply",
      { parentMessageId: MESSAGE_ID },
      { parentMessageId: MESSAGE_ID },
    ],
    [
      "an email",
      {
        email: {
          subject: "Re: Hello",
          cc: [{ address: "cc@example.invalid" }],
        },
      },
      {
        email: {
          subject: "Re: Hello",
          cc: [{ address: "cc@example.invalid" }],
        },
      },
    ],
    [
      "a Telegram message",
      { telegram: { richMessage: false } },
      { telegram: { richMessage: false } },
    ],
  ])("sends %s as sendMessage", async (_, extra, expectedExtra) => {
    const t = setup();
    const photonApi = stubApi(json(202, dispatched()));
    const turnId = await startTurn(t);

    const queued = await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "sendMessage" as const,
      content: text,
      ...extra,
      ...credentials,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(1);
    expect(photonApi.requests[0]).toMatchObject({
      url: `${BASE_URL}/v1/projects/${PROJECT_ID}/messages`,
      method: "POST",
      headers: {
        authorization: `Bearer ${API_KEY}`,
        "idempotency-key": queued.idempotencyKey,
      },
    });
    expect(photonApi.requests[0]?.body).toEqual({
      spaceId: SPACE_ID,
      senderId: AGENT_ID,
      content: text,
      ...expectedExtra,
    });
  });

  it("marks the turn's newest message read", async () => {
    const t = setup();
    const photonApi = stubApi(json(202, dispatched()));
    const turnId = await startTurn(t);

    const queued = await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "markMessageRead" as const,
      ...credentials,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(1);
    expect(photonApi.requests[0]).toMatchObject({
      url: `${BASE_URL}/v1/projects/${PROJECT_ID}/messages/${MESSAGE_ID}/read`,
      method: "POST",
      headers: {
        authorization: `Bearer ${API_KEY}`,
        "idempotency-key": queued.idempotencyKey,
      },
      body: undefined,
    });
  });

  it("sets typing in the space", async () => {
    const t = setup();
    const photonApi = stubApi(
      json(
        202,
        dispatched({ content: { type: "space.typing.update", typing: true } })
      )
    );
    const turnId = await startTurn(t);

    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "setSpaceTyping" as const,
      ...credentials,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(1);
    expect(photonApi.requests[0]).toMatchObject({
      url: `${BASE_URL}/v1/projects/${PROJECT_ID}/spaces/${SPACE_ID}/typing`,
      method: "POST",
      headers: { authorization: `Bearer ${API_KEY}` },
    });
    expect(photonApi.requests[0]?.body).toEqual({
      senderId: AGENT_ID,
      typing: true,
    });
    expect(photonApi.requests[0]?.headers).not.toHaveProperty(
      "idempotency-key"
    );
    expect((await outbox(t))[0]?.status).toBe("sent");
  });

  it("fails a read receipt with no message to mark, without a request", async () => {
    const t = setup();
    const photonApi = stubApi(json(202, dispatched()));
    await startTurn(t);

    // No turn, so there is no newest message to fall back to.
    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      kind: "markMessageRead" as const,
      ...credentials,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(0);
    expect((await outbox(t))[0]).toMatchObject({
      status: "failed",
      attempts: 1,
    });
  });
});

describe("responses", () => {
  const sendOnce = async (...responses: (() => Response)[]) => {
    const t = setup();
    const photonApi = stubApi(...responses);
    const turnId = await startTurn(t);
    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "sendMessage" as const,
      content: text,
      ...credentials,
    });
    await settle(t);
    return { photonApi, rows: await outbox(t) };
  };

  it("records the Message id of an accepted send", async () => {
    const { rows } = await sendOnce(json(202, dispatched()));
    expect(rows[0]).toMatchObject({
      status: "sent",
      attempts: 1,
      messageId: "pho_msg_00000000000000000000000009",
    });
  });

  it("treats a replayed 202 as sent", async () => {
    const { rows } = await sendOnce(
      json(202, dispatched(), { "idempotent-replayed": "true" })
    );
    expect(rows[0]).toMatchObject({ status: "sent", attempts: 1 });
  });

  it("treats a 202 that does not match the schema as sent", async () => {
    const { rows } = await sendOnce(json(202, { unexpected: true }));
    expect(rows[0]).toMatchObject({ status: "sent", attempts: 1 });
    expect(rows[0]).not.toHaveProperty("messageId");
  });

  it("fails a 202 whose Message status is failed, without retrying", async () => {
    const { photonApi, rows } = await sendOnce(json(202, failed()));
    expect(photonApi.requests).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: "failed", attempts: 1 });
    expect(rows[0]?.lastError).toContain("message_rejected");
  });

  it("reads a Message the client cannot recognise from raw", async () => {
    // Telegram typing comes back as `space.typing.update`, which 0.6.0's
    // Telegram content does not include: the client wraps it as UNKNOWN.
    const t = setup();
    stubApi(
      json(
        202,
        dispatched({
          platform: "telegram",
          content: { type: "space.typing.update", typing: true },
        })
      )
    );
    const turnId = await startTurn(t, { platform: "telegram" });
    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "setSpaceTyping" as const,
      ...credentials,
    });
    await settle(t);

    expect((await outbox(t))[0]).toMatchObject({
      status: "sent",
      attempts: 1,
      messageId: "pho_msg_00000000000000000000000009",
    });
  });

  it("fails an unrecognised Message whose raw status is failed", async () => {
    // A reaction is not iMessage content in 0.6.0, so this one is wrapped too.
    const { rows } = await sendOnce(
      json(
        202,
        failed({ content: { type: "reaction", action: "add", reaction: "👍" } })
      )
    );
    expect(rows[0]).toMatchObject({ status: "failed", attempts: 1 });
  });

  it.each([
    ["sendMessage", 422, "VALIDATION_FAILED"],
    ["sendMessage", 409, "SPACE_NOT_SENDABLE"],
    ["markMessageRead", 422, "OPERATION_UNAVAILABLE"],
    ["setSpaceTyping", 422, "OPERATION_UNAVAILABLE"],
  ] as const)("does not retry %s after %i %s", async (kind, status, code) => {
    const t = setup();
    const photonApi = stubApi(json(status, { code, status, detail: "nope" }));
    const turnId = await startTurn(t);

    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind,
      ...(kind === "sendMessage" ? { content: text } : {}),
      ...credentials,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(1);
    const [row] = await outbox(t);
    expect(row).toMatchObject({ status: "failed", attempts: 1 });
    expect(row?.lastError).toContain(`${status} ${code}`);
  });

  it.each([
    ["429", json(429, { code: "RATE_LIMITED", status: 429 })],
    ["503", json(503, { code: "SERVICE_UNAVAILABLE", status: 503 })],
    [
      "a network error",
      () => {
        throw new TypeError("fetch failed");
      },
    ],
  ])("retries after %s with the same idempotency key", async (_, first) => {
    const { photonApi, rows } = await sendOnce(first, json(202, dispatched()));

    expect(photonApi.requests).toHaveLength(2);
    const [one, two] = photonApi.requests;
    expect(two?.headers["idempotency-key"]).toBe(
      one?.headers["idempotency-key"]
    );
    expect(rows[0]).toMatchObject({ status: "sent", attempts: 2 });
  });

  it("gives up after five attempts", async () => {
    const { photonApi, rows } = await sendOnce(
      json(503, { code: "SERVICE_UNAVAILABLE", status: 503 })
    );
    expect(photonApi.requests).toHaveLength(5);
    expect(rows[0]).toMatchObject({ status: "failed", attempts: 5 });
  });
});

describe("credentials", () => {
  /** A client whose ctx runs the component's own functions in `t`. */
  const client = (
    t: ReturnType<typeof setup>,
    options: ConstructorParameters<typeof PhotonComponent>[1]
  ) => {
    const photon = new PhotonComponent(api as unknown as ComponentApi, options);
    const ctx = {
      runMutation: (reference: unknown, args: unknown) =>
        t.mutation(reference as typeof api.outbox.enqueue, args as never),
    } as unknown as Parameters<PhotonComponent["sendMessage"]>[0];
    return { photon, ctx };
  };

  const config = async (t: ReturnType<typeof setup>) =>
    await t.run(async (ctx) => await ctx.db.query("config").collect());

  it("keeps one config row, updated when the credentials change", async () => {
    const t = setup();
    stubApi(json(202, dispatched()));
    const turnId = await startTurn(t);

    const first = client(t, credentials);
    await first.photon.sendMessage(first.ctx, {
      spaceId: SPACE_ID,
      turnId,
      content: text,
    });
    await first.photon.markMessageRead(first.ctx, {
      spaceId: SPACE_ID,
      turnId,
    });
    expect(await config(t)).toMatchObject([credentials]);
    expect(await config(t)).toHaveLength(1);

    // No base URL from the shell either: the stored one must be cleared.
    vi.stubEnv("PHOTON_BASE_URL", undefined);
    const rotated = client(t, {
      projectId: PROJECT_ID,
      apiKey: "pho_sk_rotated",
    });
    await rotated.photon.sendMessage(rotated.ctx, {
      spaceId: SPACE_ID,
      turnId,
      content: text,
    });
    const rows = await config(t);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      projectId: PROJECT_ID,
      apiKey: "pho_sk_rotated",
    });
    expect(rows[0]).not.toHaveProperty("baseUrl");
  });

  it.each([
    ["apiKey", { projectId: PROJECT_ID }],
    ["projectId", { apiKey: API_KEY }],
  ])("throws without %s instead of queueing", async (_, options) => {
    vi.stubEnv("PHOTON_PROJECT_ID", "");
    vi.stubEnv("PHOTON_API_KEY", "");
    const t = setup();
    const { photon, ctx } = client(t, options);

    await expect(
      photon.sendMessage(ctx, { spaceId: SPACE_ID, content: text })
    ).rejects.toThrow(MISSING_CREDENTIALS);
    await expect(
      photon.markMessageRead(ctx, { spaceId: SPACE_ID })
    ).rejects.toThrow(MISSING_CREDENTIALS);
    expect(await outbox(t)).toHaveLength(0);
  });

  it("fails a job without a request when no credentials were ever stored", async () => {
    const t = setup();
    const photonApi = stubApi(json(202, dispatched()));
    const turnId = await startTurn(t);

    await t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "sendMessage" as const,
      content: text,
    });
    await settle(t);

    expect(photonApi.requests).toHaveLength(0);
    expect((await outbox(t))[0]).toMatchObject({
      status: "failed",
      attempts: 1,
    });
  });
});

describe("reactions", () => {
  const reaction = { type: "reaction", action: "add", reaction: "👍" };

  const enqueue = (
    t: ReturnType<typeof setup>,
    turnId: string,
    content: Record<string, unknown>,
    pacingMs = 0
  ) =>
    t.mutation(api.outbox.enqueue, {
      spaceId: SPACE_ID,
      turnId,
      kind: "sendMessage" as const,
      content,
      ...(content === reaction ? { parentMessageId: MESSAGE_ID } : {}),
      pacingMs,
      ...credentials,
    });

  const collapse = (messageId: string) => ({
    messageId,
    message: receivedMessage({ id: messageId }),
    mode: "collapse" as const,
  });

  it("does not answer the turn, so a superseded turn carries its messages", async () => {
    const t = setup();
    stubApi(json(202, dispatched({ content: reaction })));
    const turnId = await startTurn(t, collapse("m1"));

    await enqueue(t, turnId, reaction);
    await settle(t);
    expect((await outbox(t))[0]?.status).toBe("sent");
    await t.mutation(api.pipeline.completeTurn, { spaceId: SPACE_ID, turnId });

    await t.mutation(api.inbound.enqueue, {
      spaceId: SPACE_ID,
      platform: "imessage",
      ...collapse("m2"),
    });

    const carried = await t.run(
      async (ctx) => await ctx.db.query("carried").collect()
    );
    expect(carried.map((row) => row.messageId)).toEqual(["m1"]);
  });

  it("does not spend inherited messages; a text reply does", async () => {
    const t = setup();
    stubApi(json(202, dispatched()));
    await startTurn(t, collapse("m1"));
    const turnId = await startTurn(t, collapse("m2"));
    const carried = async () =>
      await t.run(async (ctx) => await ctx.db.query("carried").collect());
    expect(await carried()).toHaveLength(1);

    await enqueue(t, turnId, reaction);
    await settle(t);
    expect(await carried()).toHaveLength(1);

    await enqueue(t, turnId, text);
    await settle(t);
    expect(await carried()).toHaveLength(0);
  });

  it("is not delayed by pacingMs", async () => {
    const t = setup();
    stubApi(json(202, dispatched()));
    const turnId = await startTurn(t);
    await enqueue(t, turnId, text);

    await enqueue(t, turnId, reaction, 800);
    await enqueue(t, turnId, text, 800);

    const claims = (
      await t.run(
        async (ctx) =>
          await ctx.db.system.query("_scheduled_functions").collect()
      )
    ).filter((job) => job.name.includes("claim"));
    expect(claims).toHaveLength(3);
    // `_creationTime` carries sub-millisecond precision.
    const delays = claims.map((job) =>
      Math.max(0, Math.round(job.scheduledTime - job._creationTime))
    );
    // The first text and the reaction go at once; only the second text paces.
    expect(delays).toEqual([0, 0, 800]);
  });
});
