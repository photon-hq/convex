import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { components } from "../example/convex/_generated/api.js";
import schema from "../src/component/schema.js";
import { parseWebhook } from "../src/shared/wire.js";
import {
  AGENT_ID,
  API_KEY,
  dispatched,
  event,
  fakeFetch,
  json,
  PLATFORMS,
  PROJECT_ID,
  receivedMessage,
  SECRET,
  SPACE_ID,
  sign,
} from "./fixtures.js";
import vector from "./standard-webhooks.json";

/**
 * The webhook entry point: Standard Webhooks verification, the API version
 * gate and the event checks in `parseWebhook`, then the route the example app
 * mounts, end to end into the component's tables.
 */

const OTHER_SECRET = "whsec_YW5vdGhlci10ZXN0LW9ubHktc2lnbmluZy1rZXk=";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

const parse = (body: string, headers: Record<string, string>) =>
  parseWebhook({ body, headers, secret: SECRET });

describe("parseWebhook: signature", () => {
  const body = JSON.stringify(event(receivedMessage()));

  it("verifies the contracts test vector", () => {
    vi.setSystemTime(new Date(vector.now));
    const outcome = parseWebhook({
      body: vector.body,
      headers: vector.headers,
      secret: vector.secret,
    });
    // The signature checks out. The vector's body is not an event, so it is
    // turned away one step later: 400, not 401.
    expect(outcome).toMatchObject({ ok: false, status: 400 });
  });

  it("rejects a signature made with another key", () => {
    const outcome = parse(body, sign(body, { secret: OTHER_SECRET }));
    expect(outcome).toMatchObject({ ok: false, status: 401 });
  });

  it("rejects a body changed by one byte", () => {
    const headers = sign(body);
    const outcome = parse(body.replace("Hello", "Hellp"), headers);
    expect(outcome).toMatchObject({ ok: false, status: 401 });
  });

  it("rejects a timestamp more than five minutes old", () => {
    const at = new Date(Date.now() - 301_000);
    expect(parse(body, sign(body, { at }))).toMatchObject({
      ok: false,
      status: 401,
    });
  });

  it.each(["webhook-id", "webhook-timestamp", "webhook-signature"])(
    "rejects a delivery without %s",
    (name) => {
      const headers = sign(body);
      delete headers[name];
      expect(parse(body, headers)).toMatchObject({ ok: false, status: 401 });
    }
  );

  it("accepts a delivery when one of two signatures matches", () => {
    const headers = sign(body);
    const stale = sign(body, { secret: OTHER_SECRET })["webhook-signature"];
    headers["webhook-signature"] = `${stale} ${headers["webhook-signature"]}`;
    expect(parse(body, headers).ok).toBe(true);
  });

  it("reads the headers whatever their case", () => {
    const headers = Object.fromEntries(
      Object.entries(sign(body)).map(([name, value]) => [
        name.toUpperCase(),
        value,
      ])
    );
    expect(parse(body, headers).ok).toBe(true);
  });

  it("answers 500 for a secret that is not a signing secret", () => {
    const outcome = parseWebhook({
      body,
      headers: sign(body),
      secret: "whsec_not base64",
    });
    expect(outcome).toMatchObject({ ok: false, status: 500 });
  });
});

describe("parseWebhook: event", () => {
  const signed = (payload: unknown) => {
    const body =
      typeof payload === "string" ? payload : JSON.stringify(payload);
    return parse(body, sign(body));
  };

  it.each([
    ["not JSON", "{"],
    ["empty", ""],
    ["an array", "[]"],
    ["a string", '"hello"'],
  ])("answers 400 for a body that is %s", (_, body) => {
    expect(signed(body)).toMatchObject({ ok: false, status: 400 });
  });

  it("answers 400 for an API version this release does not read", () => {
    const outcome = signed(
      event(receivedMessage(), { apiVersion: "2026-01-01" })
    );
    expect(outcome).toMatchObject({ ok: false, status: 400 });
  });

  it("answers 400 for an event without apiVersion", () => {
    const { apiVersion: _, ...unversioned } = event(receivedMessage());
    expect(signed(unversioned)).toMatchObject({ ok: false, status: 400 });
  });

  it("answers 400 for an event without type or data.object.id", () => {
    const { type: _, ...untyped } = event(receivedMessage());
    expect(signed(untyped)).toMatchObject({ ok: false, status: 400 });
    const { id: __, ...anonymous } = receivedMessage();
    expect(signed(event(anonymous))).toMatchObject({ ok: false, status: 400 });
  });

  it.each([
    [
      "platform.sending.paused",
      {
        object: {
          platform: "email",
          reason: "reputation_policy",
          scope: "organization",
        },
        projectId: PROJECT_ID,
      },
    ],
    ["external-webhook.received", { rawRequestBase64: "e30=" }],
  ])("acknowledges %s, which has no data.object.id", (type, data) => {
    expect(signed(event(receivedMessage(), { type, data }))).toMatchObject({
      ok: false,
      status: 200,
      acknowledged: true,
    });
  });

  it("answers 400 for message.received without data.object.platform", () => {
    const { platform: _, ...unplatformed } = receivedMessage();
    expect(signed(event(unplatformed))).toMatchObject({
      ok: false,
      status: 400,
    });
  });

  it("acknowledges a message without a space, storing nothing", () => {
    const { space: _, ...spaceless } = receivedMessage();
    expect(signed(event(spaceless))).toMatchObject({
      ok: false,
      status: 200,
      acknowledged: true,
    });
  });

  it("starts a turn for a content type it does not know", () => {
    const outcome = signed(
      event(receivedMessage({ content: { type: "future.content" } }))
    );
    expect(outcome.ok && outcome.message.triggersTurn).toBe(true);
  });

  it.each([
    { type: "message.read" },
    { type: "space.typing.update", typing: true },
  ])("does not start a turn for $type", (content) => {
    const outcome = signed(event(receivedMessage({ content })));
    expect(outcome.ok).toBe(true);
    expect(outcome.ok && outcome.message.triggersTurn).toBe(false);
  });
});

describe("the webhook route", () => {
  // The example app is the host: it mounts `registerRoutes` and answers each
  // turn with `markMessageRead` and `sendMessage`.
  const exampleModules = import.meta.glob("../example/convex/**/*.*s");
  const componentModules = import.meta.glob("../src/component/**/*.ts");

  const setup = () => {
    const t = convexTest(undefined, exampleModules);
    t.registerComponent("photon", schema, componentModules);
    return t;
  };

  let photonApi: ReturnType<typeof fakeFetch>;

  beforeEach(() => {
    vi.stubEnv("PHOTON_PROJECT_ID", PROJECT_ID);
    vi.stubEnv("PHOTON_API_KEY", API_KEY);
    vi.stubEnv("PHOTON_WEBHOOK_SECRET", SECRET);
    photonApi = fakeFetch(json(202, dispatched()));
    vi.stubGlobal("fetch", photonApi.fetch);
  });

  const deliver = async (
    t: ReturnType<typeof setup>,
    payload: Record<string, unknown>
  ) => {
    const body = JSON.stringify(payload);
    return await t.fetch("/photon/webhook", {
      method: "POST",
      body,
      headers: sign(body, { id: String(payload.id) }),
    });
  };

  const settle = async (t: ReturnType<typeof setup>): Promise<void> => {
    await t.finishAllScheduledFunctions(vi.runAllTimers);
  };

  const stored = async (t: ReturnType<typeof setup>) =>
    await t.query(components.photon.inbound.listMessages, {
      spaceId: SPACE_ID,
    });

  const sends = () =>
    photonApi.requests.filter((request) =>
      request.url.endsWith(`/v1/projects/${PROJECT_ID}/messages`)
    );

  it.each(PLATFORMS)(
    "stores a %s text message as delivered",
    async (platform) => {
      const t = setup();
      const object = receivedMessage({ platform });

      const response = await deliver(t, event(object));

      expect(response.status).toBe(200);
      const rows = await stored(t);
      expect(rows).toHaveLength(1);
      expect(rows[0]?.messageId).toBe(object.id);
      expect(rows[0]?.message).toEqual(object);
      expect(
        await t.query(components.photon.inbound.getSpace, { spaceId: SPACE_ID })
      ).toMatchObject({
        spaceId: SPACE_ID,
        platform,
        senderId: AGENT_ID,
        kind: "direct",
      });
    }
  );

  it("answers 401 to a forged delivery and stores nothing", async () => {
    const t = setup();
    const body = JSON.stringify(event(receivedMessage()));

    const response = await t.fetch("/photon/webhook", {
      method: "POST",
      body,
      headers: sign(body, { secret: OTHER_SECRET }),
    });

    expect(response.status).toBe(401);
    expect(await stored(t)).toHaveLength(0);
  });

  it("queues a message whose content type it does not know", async () => {
    const t = setup();
    await deliver(
      t,
      event(receivedMessage({ content: { type: "future.content" } }))
    );
    expect((await stored(t))[0]?.status).toBe("queued");
  });

  it.each([
    { type: "message.read" },
    { type: "space.typing.update", typing: true },
  ])("stores $type already handled", async (content) => {
    const t = setup();
    await deliver(t, event(receivedMessage({ content })));
    await settle(t);
    expect((await stored(t))[0]?.status).toBe("handled");
    expect(photonApi.requests).toHaveLength(0);
  });

  it("acknowledges other event types without storing them", async () => {
    const t = setup();
    const delivered = { ...dispatched(), status: "delivered" };

    const response = await deliver(
      t,
      event(delivered, { type: "message.delivered" })
    );

    expect(response.status).toBe(200);
    expect(await stored(t)).toHaveLength(0);
  });

  it("keeps the space's senderId when an event has no receivedBy", async () => {
    const t = setup();
    await deliver(t, event(receivedMessage()));
    const { receivedBy: _, ...anonymous } = receivedMessage({
      id: "pho_msg_00000000000000000000000002",
    });

    await deliver(
      t,
      event(anonymous, { id: "pho_evt_00000000000000000000000002" })
    );

    expect(await stored(t)).toHaveLength(2);
    expect(
      (await t.query(components.photon.inbound.getSpace, { spaceId: SPACE_ID }))
        ?.senderId
    ).toBe(AGENT_ID);
  });

  it("fails a reply without retrying when no event ever named the sender", async () => {
    const t = setup();
    const { receivedBy: _, ...anonymous } = receivedMessage();

    await deliver(t, event(anonymous));
    await settle(t);

    const outbox = await t.query(components.photon.outbox.list, {
      spaceId: SPACE_ID,
    });
    expect(outbox.find((row) => row.kind === "sendMessage")).toMatchObject({
      status: "failed",
      attempts: 1,
    });
    expect(sends()).toHaveLength(0);
  });

  it("stores a redelivered event once and runs the handler once", async () => {
    const t = setup();
    const payload = event(receivedMessage());

    expect((await deliver(t, payload)).status).toBe(200);
    expect((await deliver(t, payload)).status).toBe(200);
    await settle(t);

    expect(await stored(t)).toHaveLength(1);
    expect(sends()).toHaveLength(1);
  });

  it("answers 500 when no webhook secret is configured", async () => {
    vi.stubEnv("PHOTON_WEBHOOK_SECRET", "");
    const t = setup();
    expect((await deliver(t, event(receivedMessage()))).status).toBe(500);
  });
});
