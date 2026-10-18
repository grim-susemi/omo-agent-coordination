import assert from "node:assert/strict";
export default async function acceptanceConsumer(pi: any) {
  const global = globalThis as any, key = Symbol.for("omo.agent-coordination.capture.v1");
  const count = global[key]?.capture.list().length ?? 0;
  const sdk = await import("@code-yeongyu/senpi" as string), hook = sdk.AgentSession.prototype.bindExtensions;
  const specifier = "coordination-question-owned-candidate/api";
  const helper = await import(specifier);
  const captureDelta = (global[key]?.capture.list().length ?? 0) - count;
  assert.equal(captureDelta, 0); assert.equal(sdk.AgentSession.prototype.bindExtensions, hook);
  pi.registerTool({
    name: "acceptance_api_consumer", label: "Final API consumer", description: "Exercise the packaged scoped client.",
    parameters: { type: "object", properties: { target_ref: { type: "string" } }, required: ["target_ref"] },
    async execute(_id: string, args: any, _signal: any, _update: any, ctx: any) {
      const got = helper.getCoordinationClient(ctx); assert(got.ok); const client = got.value;
      const broker = helper.getCoordinationBroker(); assert(broker.ok);
      assert(broker.value.features.includes("revival_settlement_repair"));
      const revisions: number[] = [], stop = client.onPeersChanged((roster: any) => revisions.push(roster.revision));
      const peers = client.peers(); assert(peers.ok);
      const input = { target_ref: args.target_ref, question: "NONCE:consumer-nonce", request_id: "final-consumer" };
      const count = global[Symbol.for("coordination.acceptance.counts")].side;
      const reply = await client.ask(input), duplicate = await client.ask(input);
      assert(reply.ok && reply.value.reply === "consumer-nonce"); assert.deepEqual(duplicate, reply);
      assert.equal(global[Symbol.for("coordination.acceptance.counts")].side, count + 1);
      const note = await client.notify({ target_ref: args.target_ref, message: "consumer-note", request_id: "consumer-note" });
      assert(note.ok);
      const inspected = await client.inspect(args.target_ref); assert(inspected.ok && inspected.value.live);
      assert(client.setAskPolicy({ target_ref: args.target_ref, policy: "deny_peer_asks" }).ok);
      client.peers(); client.peers(); assert.equal(revisions.length, 1);
      stop(); client.dispose();
      assert.deepEqual(await client.inspect(args.target_ref), { ok: false, reason: "stale_generation" });
      return { content: [{ type: "text", text: "Final scoped API verified." }], details: {
        passed: true, features: broker.value.features, capture_delta: captureDelta, hook_unchanged: true,
        roster_revision: peers.value.revision, changed_revisions: revisions, reply, note, inspected,
        duplicate_provider_calls: 1, stale_client_refused: true,
      } };
    },
  });
}
