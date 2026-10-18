import assert from "node:assert/strict";

export default async function questionConsumer(pi: any) {
  const global = globalThis as any, key = Symbol.for("omo.agent-coordination.capture.v1");
  const before = global[key], count = before?.capture.list().length ?? 0;
  const provider = global[Symbol.for("coordination.question.qa.counts")]?.side ?? 0;
  const sdkName = "@code-yeongyu/senpi";
  const sdk = await import(sdkName);
  const hook = sdk.AgentSession.prototype.bindExtensions;
  const specifier = "coordination-question-owned-candidate/api";
  const helper = await import(specifier);
  const pure = { capture_delta: (global[key]?.capture.list().length ?? 0) - count,
    provider_delta: (global[Symbol.for("coordination.question.qa.counts")]?.side ?? 0) - provider,
    hook_unchanged: sdk.AgentSession.prototype.bindExtensions === hook };
  pi.registerTool({
    name: "question_api_consumer", label: "Question API consumer", description: "Consume the built pure API helper from a separate extension.",
    parameters: { type: "object", properties: { target_ref: { type: "string" } }, required: ["target_ref"] },
    async execute(_id: string, args: any, _signal: unknown, _update: unknown, ctx: unknown) {
      const connected = helper.getCoordinationClient(ctx); assert(connected.ok);
      const client = connected.value;
      const broker = helper.getCoordinationBroker(); assert(broker.ok);
      assert.deepEqual(broker.value.features, ["ask_depth_deep", "ask_policy", "inspect"]);
      const revisions: number[] = [];
      const stop = client.onPeersChanged((roster: any) => revisions.push(roster.revision));
      const beforeCount = global[Symbol.for("coordination.question.qa.counts")].side;
      const input = { target_ref: args.target_ref, request_id: "consumer-duplicate", question: "NONCE:consumer-nonce" };
      const reply = await client.ask(input); assert(reply.ok);
      const duplicate = await client.ask(input); assert(duplicate.ok);
      assert.equal(reply.value.reply, "consumer-nonce");
      assert.equal(duplicate.value.reply, reply.value.reply);
      assert.equal(global[Symbol.for("coordination.question.qa.counts")].side, beforeCount + 1);
      const note = await client.notify({ target_ref: args.target_ref, request_id: "consumer-note", message: "consumer-note-nonce" });
      assert(note.ok);
      const inspected = await client.inspect(args.target_ref); assert(inspected.ok && inspected.value.live === true);
      const policy = client.setAskPolicy({ target_ref: args.target_ref, policy: "deny_peer_asks" }); assert(policy.ok);
      client.peers(); client.peers();
      assert.equal(revisions.length, 1);
      stop(); client.dispose();
      assert.deepEqual(await client.inspect(args.target_ref), { ok: false, reason: "stale_generation" });
      assert.equal(pure.capture_delta, 0); assert.equal(pure.provider_delta, 0); assert(pure.hook_unchanged);
      const result = { passed: true, ...pure, duplicate_provider_calls: 1, features: broker.value.features,
        subscribed_revisions: revisions, stale_client_refused: true, reply: reply.value };
      return { content: [{ type: "text", text: JSON.stringify(result) }], details: result };
    },
  });
}
