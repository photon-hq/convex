import { describe, expect, it } from "vitest";
import { pickTurn } from "../src/client/index.js";

/**
 * Regression guard for a bug that unit tests missed and a live deployment
 * caught: handlers are naturally called as `photon.isCancelled(ctx, args)`
 * with the *whole* turn, but Convex validators reject unknown fields. If the
 * client stops narrowing, every real turn handler throws
 * `ArgumentValidationError` at runtime.
 */
describe("pickTurn", () => {
  it("drops turn fields the component's validators would reject", () => {
    const turn = {
      spaceId: "chat-1",
      turnId: "chat-1:123:abc",
      messages: [
        {
          id: "m1",
          content: { type: "text", format: "plain", text: "hi" },
        },
      ],
      carried: [],
    };
    expect(pickTurn(turn)).toEqual({
      spaceId: "chat-1",
      turnId: "chat-1:123:abc",
    });
    expect(Object.keys(pickTurn(turn))).toHaveLength(2);
  });
});
