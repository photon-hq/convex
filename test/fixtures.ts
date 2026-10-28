import { Webhook } from "standardwebhooks";

/**
 * Event and response fixtures in the shape of the `2026-07-01` contract
 * examples (`photon-hq/contracts`, `schemas/2026-07-01/contract.json`), plus
 * the `receivedBy` user those examples leave out.
 */

export const PROJECT_ID = "pho_prj_00000000000000000000000001";
export const SPACE_ID = "pho_spc_00000000000000000000000001";
export const AGENT_ID = "pho_usr_00000000000000000000000002";
export const API_KEY = "pho_sk_test";

/** A test-only signing key — never a real destination's. */
export const SECRET = "whsec_dGVzdC1vbmx5LXNpZ25pbmcta2V5LWZvci1jb252ZXg=";

export const PLATFORMS = [
  "imessage",
  "sms",
  "whatsapp",
  "whatsapp_business",
  "email",
  "telegram",
] as const;
export type Platform = (typeof PLATFORMS)[number];

const HANDLES: Record<Platform, { agent: string; sender: string }> = {
  imessage: { sender: "+12025550101", agent: "+12025550102" },
  sms: { sender: "+12025550101", agent: "+12025550102" },
  whatsapp: { sender: "+12025550101", agent: "+12025550102" },
  whatsapp_business: { sender: "+12025550101", agent: "+12025550102" },
  email: { sender: "sender@example.invalid", agent: "agent@example.invalid" },
  telegram: { sender: "123456789", agent: "987654321" },
};

const TIMESTAMP = "2026-07-01T12:00:00.000Z";

const user = (
  platform: Platform,
  id: string,
  handle: string,
  kind: string
) => ({
  details: {},
  handle,
  id,
  kind,
  metadata: {},
  platform,
  projectId: PROJECT_ID,
});

/** A received `Message` — the `data.object` of `message.received`. */
export const receivedMessage = (
  overrides: { platform?: Platform; [key: string]: unknown } = {}
): Record<string, unknown> => {
  const platform = overrides.platform ?? "imessage";
  return {
    content: { format: "plain", text: "Hello", type: "text" },
    createdAt: TIMESTAMP,
    id: "pho_msg_00000000000000000000000001",
    metadata: {},
    platform,
    projectId: PROJECT_ID,
    receivedBy: user(platform, AGENT_ID, HANDLES[platform].agent, "agent"),
    sentBy: user(
      platform,
      "pho_usr_00000000000000000000000001",
      HANDLES[platform].sender,
      "human"
    ),
    space: {
      details: {},
      id: SPACE_ID,
      kind: "direct",
      metadata: {},
      participantIds: [],
      platform,
      projectId: PROJECT_ID,
    },
    status: "received",
    timestamps: { receivedAt: TIMESTAMP },
    updatedAt: TIMESTAMP,
    ...overrides,
  };
};

/** A webhook event envelope around `object`. */
export const event = (
  object: Record<string, unknown>,
  overrides: Record<string, unknown> = {}
): Record<string, unknown> => ({
  apiVersion: "2026-07-01",
  data: { object, projectId: PROJECT_ID },
  id: "pho_evt_00000000000000000000000001",
  source: `${String(object.platform)}.photon`,
  timestamp: TIMESTAMP,
  type: "message.received",
  ...overrides,
});

/** Standard Webhooks headers for `body`, signed at the current (fake) time. */
export const sign = (
  body: string,
  options: { secret?: string; id?: string; at?: Date } = {}
): Record<string, string> => {
  const id = options.id ?? "pho_evt_00000000000000000000000001";
  const at = options.at ?? new Date();
  return {
    "webhook-id": id,
    "webhook-timestamp": String(Math.floor(at.getTime() / 1000)),
    "webhook-signature": new Webhook(options.secret ?? SECRET).sign(
      id,
      at,
      body
    ),
  };
};

/** The 202 body of a send: the dispatched `Message`. */
export const dispatched = (
  overrides: Record<string, unknown> = {}
): Record<string, unknown> => ({
  content: { format: "plain", text: "echo: Hello", type: "text" },
  createdAt: TIMESTAMP,
  id: "pho_msg_00000000000000000000000009",
  metadata: {},
  platform: "imessage",
  projectId: PROJECT_ID,
  sentBy: user("imessage", AGENT_ID, HANDLES.imessage.agent, "agent"),
  space: {
    details: {},
    id: SPACE_ID,
    kind: "direct",
    metadata: {},
    participantIds: [],
    platform: "imessage",
    projectId: PROJECT_ID,
  },
  status: "dispatched",
  timestamps: { dispatchedAt: TIMESTAMP },
  updatedAt: TIMESTAMP,
  ...overrides,
});

/** A 202 body whose Message already failed. */
export const failed = (
  overrides: Record<string, unknown> = {}
): Record<string, unknown> =>
  dispatched({
    failure: { code: "message_rejected", message: "The message was rejected." },
    status: "failed",
    timestamps: { failedAt: TIMESTAMP },
    ...overrides,
  });

/** One request the stubbed `fetch` received. */
export interface CapturedRequest {
  body: unknown;
  headers: Record<string, string>;
  method: string;
  url: string;
}

/**
 * A `fetch` stand-in for the Photon API: records each request and answers
 * with the next scripted response (the last one repeats).
 */
export const fakeFetch = (
  ...responses: (() => Response)[]
): {
  fetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  requests: CapturedRequest[];
} => {
  const requests: CapturedRequest[] = [];
  const fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit
  ): Promise<Response> => {
    const request = new Request(input, init);
    const text = await request.text();
    requests.push({
      url: request.url,
      method: request.method,
      headers: Object.fromEntries(request.headers),
      body: text === "" ? undefined : JSON.parse(text),
    });
    const next = responses[Math.min(requests.length, responses.length) - 1];
    if (!next) {
      throw new Error("fakeFetch: no response scripted");
    }
    return next();
  };
  return { fetch, requests };
};

export const json =
  (
    status: number,
    body: unknown,
    headers: Record<string, string> = {}
  ): (() => Response) =>
  () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json", ...headers },
    });
