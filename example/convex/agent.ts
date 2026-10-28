import { v } from "convex/values";
import { internalAction } from "./_generated/server";
import { photon } from "./photon";

/**
 * Echo bot — one invocation per turn.
 *
 * `messages` and `carried` are Photon `Message`s. `carried` holds messages an
 * earlier, cancelled turn had already drained (collapse mode only). They are
 * older than `messages`, so they are labelled as prior context rather than
 * folded into the current turn.
 */
export const respond = internalAction({
  args: {
    spaceId: v.string(),
    turnId: v.string(),
    messages: v.array(v.any()),
    carried: v.array(v.any()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const render = (m: { content?: unknown }): string => {
      const c = m.content as { type?: string; text?: string } | undefined;
      return c?.type === "text" ? (c.text ?? "") : `[${c?.type ?? "unknown"}]`;
    };

    const lines = [
      ...args.carried.map((m) => `[Earlier] ${render(m)}`),
      ...args.messages.map(render),
    ];
    const count = args.messages.length + args.carried.length;
    const reply =
      count > 1
        ? `echo (${count} messages):\n${lines.join("\n")}`
        : `echo: ${lines[0] ?? ""}`;

    await photon.markMessageRead(ctx, args);

    // A real agent calls an LLM here. Anything slow means a newer message may
    // have arrived meanwhile — the scheduler cannot interrupt a running action,
    // so re-check before speaking. Only collapse mode cancels turns.
    if (await photon.isCancelled(ctx, args)) {
      return null;
    }

    await photon.sendMessage(ctx, {
      spaceId: args.spaceId,
      turnId: args.turnId,
      content: { type: "text", format: "plain", text: reply },
    });
    await photon.completeTurn(ctx, args);
    return null;
  },
});
