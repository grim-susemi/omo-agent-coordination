import assert from "node:assert/strict";
import type { CoordinationClient, Result, TargetRef } from "../../src/contracts.ts";
import type { OwnedQuestions } from "../../src/peer-questions.ts";

export async function verifyQuestionPolicyReplay(options: {
  client: CoordinationClient;
  context: unknown;
  service: OwnedQuestions;
  target: { ref: TargetRef; runtime: any };
  counts: { side: number };
  toolAsk(ref: TargetRef, id: string, question: string): Promise<any>;
}) {
  const { client, context, service, target, counts, toolAsk } = options;
  const input = { target_ref: target.ref, request_id: "adversarial-policy-replay",
    question: "NONCE:policy-replay" };
  const note = { target_ref: target.ref, request_id: "adversarial-policy-note", message: "policy replay note" };
  const first = await toolAsk(target.ref, input.request_id, input.question);
  assert(first.ok && first.value.state === "replied");
  assert.equal(first.value.reply, "policy-replay");
  assert((await client.notify(note)).ok);
  const calls = counts.side;
  assert.deepEqual(await client.ask(input), first);
  assert.equal(counts.side, calls);
  assert(client.setAskPolicy({ target_ref: target.ref, policy: "deny_peer_asks" }).ok);
  const projection = await client.inspect(target.ref);
  assert(projection.ok && projection.value.asks === "denied");
  const queued = target.runtime._pendingNextTurnMessages.length;
  assert.equal(queued, 0);
  const transport = await service.transport();
  assert(transport.ok);
  const denied: Result<never> = { ok: false, reason: "target_excluded" };
  const transportAsk = await transport.value.request(context, { target_ref: target.ref,
    request_id: input.request_id, operation: "ask", payload: { ...input, depth: "quick" } });
  assert.deepEqual(transportAsk, denied, "TRANSPORT_POLICY_REPLAY_BYPASSES_CURRENT_AUTHORIZATION");
  const registeredAsk = await toolAsk(target.ref, input.request_id, input.question);
  assert.deepEqual(registeredAsk, denied, "POLICY_REPLAY_BYPASSES_CURRENT_AUTHORIZATION");
  assert.deepEqual(await client.ask(input), denied);
  assert.deepEqual(await client.notify(note), denied);
  assert.deepEqual(await transport.value.request(context, { target_ref: target.ref,
    request_id: note.request_id, operation: "notify", payload: note }), denied);
  assert.equal(counts.side, calls);
  assert.equal(target.runtime._pendingNextTurnMessages.length, queued);
  const caller = service.bindContext(context);
  assert(caller.ok);
  assert(client.setAskPolicy({ target_ref: target.ref, policy: "deny_peer_asks",
    allow_from: [caller.value.target_ref] }).ok);
  const allowed = await toolAsk(target.ref, input.request_id, input.question);
  assert(allowed.ok && allowed.value.reply === first.value.reply);
  assert.equal(allowed.value.request_id, input.request_id);
  assert((await client.notify(note)).ok);
  assert(client.setAskPolicy({ target_ref: target.ref, policy: "default" }).ok);
  const restored = await client.ask(input);
  assert(restored.ok && restored.value.reply === first.value.reply);
  assert.equal(counts.side, calls);
  assert.equal(target.runtime._pendingNextTurnMessages.length, queued);
  return { first, asks_while_denied: projection.value.asks, registered_ask: registeredAsk,
    transport_ask: transportAsk, api_ask_and_notify_denied: true,
    provider_calls_during_replay: counts.side - calls,
    queue_additions_during_replay: target.runtime._pendingNextTurnMessages.length - queued,
    allow_from_cached_result_preserved: true, default_cached_result_preserved: true };
}
