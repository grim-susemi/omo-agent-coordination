import { expect, test } from "bun:test";
import { connectManagedQuestionChild, createPeerQuestions, createRemoteQuestionOwner, nativeQuestionTarget, sideThinkingOptions } from "../src/peer-questions.ts";
import fs from "node:fs";
import path from "node:path";
import type { PeerExchange, QuestionTarget, SideSnapshot } from "../src/peer-questions.ts";
import type { AskResult, Result, TargetBinding, TargetRef, TerminalReason } from "../src/contracts.ts";
import { runQuestionCase } from "../qa/cases/question-native.ts";
import { questionEvidence } from "../qa/cases/question-receipts.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}
function value<T>(result: Result<T>): T {
  if (!result.ok) throw new Error(result.reason);
  return result.value;
}
type Json = Record<string, any>;
function fixture() {
  let clock = 1000;
  const timers = new Set<{ at: number; callback(): void }>();
  const events: string[] = [];
  const rows = new Map<string, ReturnType<typeof target>>();
  function target(name: string, kind: "root" | "worker" = "worker") {
    const binding: TargetBinding = {
      kind, target_ref: name as TargetRef, owner_root_id: "owner", owner_session_id: "owner",
      canonical_session_id: name, session_id: name, runtime_instance_id: name, generation: 1,
      ...(kind === "root" ? { branch: "base" } : { task_id: name, run_epoch: 1 }),
    } as TargetBinding;
    const started = deferred<void>(), prepared = deferred<void>(), aborted = deferred<void>();
    const output = deferred<{ text: string; toolCalls: boolean; completion: "complete" | "truncated_by_provider";
      usage: { input: number; output: number; reasoning: number; cache: number }; attempts: number }>();
    let preparationGate: Promise<void> = Promise.resolve();
    const listeners = new Set<(reason?: TerminalReason) => void>();
    const queue: PeerExchange[] = [];
    const incorporated = new Set<string>();
    const state = {
      reason: undefined as TerminalReason | undefined, calls: 0, preparations: 0,
      revision: 1, branch: "base", thinking: "max", model: "model",
      pendingSeen: [] as readonly PeerExchange[],
    };
    const read = (): Result<SideSnapshot> => state.reason ? { ok: false, reason: state.reason } : { ok: true,
      value: { revision: state.revision, at: new Date(clock).toISOString(), branch: state.branch,
        model: { id: state.model }, thinking: state.thinking, context: { messages: [] } } };
    const adapter: QuestionTarget = {
      binding, read,
      async prepare(_snapshot, _question, pending, depth, signal) {
        state.preparations++; state.pendingSeen = pending; prepared.resolve();
        await preparationGate;
        return { ok: true, value: {
          model: state.model, route: "native-test", controls: { maxTokens: depth === "deep" ? 32000 : 4096 },
          thinking: depth === "deep" ? state.thinking : "low", reasoningBudget: null,
          outputBudget: depth === "deep" ? 32000 : 4096,
          run() {
            state.calls++;
            signal.addEventListener("abort", () => aborted.resolve(), { once: true });
            started.resolve();
            return output.promise;
          },
        } };
      },
      async queue(exchange) { queue.push(exchange); },
      remove(ids) { for (let i = queue.length - 1; i >= 0; i--) if (ids.has(queue[i]!.id)) queue.splice(i, 1); },
      consumed: (id) => incorporated.has(id),
      subscribe(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    };
    return { binding, state, adapter, queue, started: started.promise, prepared: prepared.promise,
      aborted: aborted.promise,
      prepareGate(promise: Promise<void>) { preparationGate = promise; },
      finish(text = "nonce", toolCalls = false, completion: "complete" | "truncated_by_provider" = "complete") {
        output.resolve({ text, toolCalls, completion, usage: { input: 3, output: 4, reasoning: 2, cache: 1 }, attempts: 1 });
      },
      fail() { output.reject(new Error("controlled provider failure")); },
      emit(reason?: TerminalReason) { state.reason = reason; for (const listener of [...listeners]) listener(reason); },
      consume() { for (const row of queue.splice(0)) incorporated.add(row.id);
        for (const listener of [...listeners]) listener(); },
    };
  }
  for (const name of ["root", "a", "b", "c", "d", "e"]) rows.set(name, target(name, name === "root" ? "root" : "worker"));
  const q = createPeerQuestions({ generation: 1, now: () => clock,
    schedule(ms, callback) {
      const timer = { at: clock + ms, callback }; timers.add(timer);
      return () => { timers.delete(timer); };
    },
    resolve(binding) {
      const row = rows.get(binding.target_ref);
      if (!row || row.binding.runtime_instance_id !== binding.runtime_instance_id) return { ok: false, reason: "invalid_target" };
      const current = row.adapter.read();
      return current.ok ? { ok: true, value: row.adapter } : current;
    },
    onReceipt(receipt) { events.push(receipt.state); },
  });
  const root = rows.get("root")!, a = rows.get("a")!, b = rows.get("b")!;
  const ask = (id = "request", extra: Record<string, unknown> = {}, caller = root, recipient = a) =>
    q.ask(caller.binding, recipient.binding, { target_ref: recipient.binding.target_ref,
      question: "Return nonce", request_id: id, ...extra } as any);
  return { q, rows, root, a, b, ask, events,
    advance(ms: number, fire = true) { clock += ms;
      if (fire) for (const timer of [...timers]) if (timer.at <= clock) { timers.delete(timer); timer.callback(); } },
  };
}

test("accepted, delivered, replied and next-turn incorporation are independent and duplicate calls execute once", async () => {
  const f = fixture();
  const result = f.ask(), duplicate = f.ask();
  await f.a.started;
  expect(f.events).toEqual(["accepted", "delivered"]);
  expect(f.a.state.calls).toBe(1);
  f.a.state.revision++;
  f.a.finish();
  const reply = value(await result);
  expect(value(await duplicate)).toBe(reply);
  expect(reply).toMatchObject({ state: "replied", reply: "nonce", current_revision: 2,
    incorporation: "queued_for_next_turn", ended_by: "reply", logical_provider_requests: 1 });
  expect(f.a.queue).toHaveLength(1);
  f.a.consume();
  expect(reply.incorporation).toBe("incorporated");
  f.a.consume();
  expect(f.a.queue).toHaveLength(0);
  f.q.dispose();
});

test("quick absolute deadline wins even when the timer callback is delayed past provider resolution", async () => {
  const f = fixture(), pending = f.ask("deadline", { timeout_ms: 100 });
  await f.a.started;
  f.advance(101, false);
  f.a.finish("too late");
  const result = value(await pending);
  expect(result.state).toBe("timed_out");
  expect(result.reply).toBeUndefined();
  expect(f.a.queue).toHaveLength(0);
  expect(f.q.diagnostics().active).toBe(0);
  f.q.dispose();
});

test("a target that settles during preparation receives no provider request", async () => {
  const f = fixture(), gate = deferred<void>();
  f.a.prepareGate(gate.promise);
  const pending = f.ask();
  await f.a.prepared;
  f.a.emit("target_settled");
  gate.resolve();
  expect(value(await pending)).toMatchObject({ state: "cancelled", ended_by: "target_settled",
    logical_provider_requests: 0 });
  expect(f.a.state.calls).toBe(0);
  f.q.dispose();
});

test("terminal, forbidden, and stale targets are refused before preparation", async () => {
  for (const reason of ["target_settled", "scope_denied", "stale_epoch", "target_not_live"] as const) {
    const f = fixture();
    f.a.state.reason = reason;
    expect(await f.ask()).toEqual({ ok: false, reason });
    expect(f.a.state.preparations).toBe(0);
    expect(f.a.state.calls).toBe(0);
    f.q.dispose();
  }
});

test("depth is explicit, deep plus a timeout is invalid, and different content under one ID conflicts", async () => {
  const f = fixture();
  expect(await f.ask("invalid", { depth: "deep", timeout_ms: 1 })).toEqual({ ok: false, reason: "invalid_argument" });
  const pending = f.ask("same", { depth: "deep" });
  await f.a.started;
  expect(await f.ask("same", { depth: "quick" })).toEqual({ ok: false, reason: "conflict" });
  expect(await f.ask("other", { depth: "deep" })).toEqual({ ok: false, reason: "target_busy" });
  f.advance(1_000_000);
  f.a.finish("x".repeat(70000), false, "truncated_by_provider");
  expect(value(await pending)).toMatchObject({ state: "replied", depth: "deep",
    reply: "x".repeat(70000), completion: "truncated_by_provider", effective_side_output_budget: 32000,
    effective_side_thinking: "max", target_thinking_before: "max" });
  f.q.dispose();
});

test("asker cancellation interrupts only the side request and discards late output and occupancy", async () => {
  const f = fixture(), controller = new AbortController();
  const pending = f.ask("abort", { depth: "deep", signal: controller.signal });
  await f.a.started;
  controller.abort();
  await f.a.aborted;
  const result = value(await pending);
  expect(result).toMatchObject({ state: "cancelled", ended_by: "asker_aborted" });
  f.a.finish("late");
  expect(result.reply).toBeUndefined();
  expect(f.a.queue).toHaveLength(0);
  expect(f.q.diagnostics()).toMatchObject({ active: 0, pending: 0 });
  f.q.dispose();
});

test("owner cancel and generation change end deep requests without replay", async () => {
  for (const ending of ["owner_cancel", "generation_change"] as const) {
    const f = fixture(), pending = f.ask(ending, { depth: "deep" });
    await f.a.started;
    if (ending === "owner_cancel") f.q.cancelCaller(f.root.binding.target_ref); else f.q.dispose();
    expect(value(await pending)).toMatchObject({ state: "cancelled", ended_by: ending });
    await f.a.aborted;
    f.a.finish("late");
    expect(f.a.state.calls).toBe(1);
    expect(f.a.queue).toHaveLength(0);
    f.q.dispose();
  }
});

test("model and branch switches cancel, but ordinary revision progress remains valid", async () => {
  for (const changed of ["model", "branch"] as const) {
    const f = fixture(), pending = f.ask(changed);
    await f.a.started;
    f.a.state[changed] = "other";
    f.a.emit();
    await f.a.aborted;
    f.a.finish();
    expect(value(await pending)).toMatchObject({ state: "cancelled", terminal_reason: "stale_epoch" });
    f.q.dispose();
  }
});

test("target loss after delivery and provider failure each commit exactly one terminal", async () => {
  for (const reason of ["target_not_live", "provider_failure"] as const) {
    const f = fixture(), pending = f.ask(reason, { depth: "deep" });
    await f.a.started;
    if (reason === "target_not_live") f.a.emit(reason); else f.a.fail();
    const reply = value(await pending);
    expect(reply.ended_by).toBe(reason);
    expect(reply.state).toBe(reason === "provider_failure" ? "failed" : "cancelled");
    expect(f.events.filter((state) => ["failed", "cancelled"].includes(state))).toHaveLength(1);
    expect(f.a.queue).toHaveLength(0);
    f.q.dispose();
    if (reason === "target_not_live") f.a.finish("late");
  }
});

test("notify calls no provider, contributes only pending peer context, and drops at shutdown", async () => {
  const f = fixture();
  const note = { request_id: "note", message: "peer evidence" };
  const receipt = value(await f.q.notify(f.root.binding, f.a.binding, note));
  expect(value(await f.q.notify(f.root.binding, f.a.binding, note))).toBe(receipt);
  expect(f.a.queue).toHaveLength(1);
  expect(f.a.state.calls).toBe(0);
  const pending = f.ask();
  await f.a.started;
  expect(f.a.state.pendingSeen).toHaveLength(1);
  f.a.finish();
  await pending;
  f.a.emit("target_settled");
  expect(receipt.incorporation).toBe("dropped");
  expect(f.a.queue).toHaveLength(0);
  f.q.dispose();
});

test("policy blocks before preparation and queue writes, but target's outbound question is allowed", async () => {
  const f = fixture(), owner = {};
  expect(f.q.setAskPolicy(f.root.binding, f.a.binding, {
    target_ref: f.a.binding.target_ref, policy: "deny_peer_asks",
  }, owner)).toEqual({ ok: true, value: { asks: "denied" } });
  expect(await f.ask("denied", {}, f.b)).toEqual({ ok: false, reason: "target_excluded" });
  expect(await f.q.notify(f.b.binding, f.a.binding, { request_id: "denied-note", message: "no" }))
    .toEqual({ ok: false, reason: "target_excluded" });
  expect(f.a.state.preparations).toBe(0);
  expect(f.a.queue).toHaveLength(0);
  const outgoing = f.ask("outbound", {}, f.a, f.root);
  await f.root.started;
  f.root.finish();
  expect(value(await outgoing).state).toBe("replied");
  f.q.disposeClient(owner);
  expect(f.q.asks(f.a.binding.target_ref)).toBe("allowed");
  f.q.dispose();
});

test("policy replacement cancels disallowed in-flight work, removes only peer queue entries, and enforces owner authority", async () => {
  const f = fixture();
  const note = value(await f.q.notify(f.b.binding, f.a.binding, { request_id: "pending-note", message: "evidence" }));
  const pending = f.ask("policy-race", { depth: "deep" }, f.b);
  await f.a.started;
  expect(f.q.setAskPolicy(f.b.binding, f.a.binding, {
    target_ref: f.a.binding.target_ref, policy: "default",
  }, {})).toEqual({ ok: false, reason: "policy_not_owner" });
  expect(f.q.setAskPolicy({ ...f.root.binding, owner_root_id: "foreign" }, f.a.binding, {
    target_ref: f.a.binding.target_ref, policy: "default",
  }, {})).toEqual({ ok: false, reason: "policy_not_owner" });
  expect(f.q.setAskPolicy(f.root.binding, f.a.binding, {
    target_ref: f.a.binding.target_ref, policy: "deny_peer_asks", allow_from: [f.root.binding.target_ref],
  }, {}).ok).toBe(true);
  expect(value(await pending)).toMatchObject({ state: "cancelled", terminal_reason: "target_excluded" });
  expect(note.incorporation).toBe("dropped");
  expect(f.a.queue).toHaveLength(0);
  f.a.finish("late");
  f.q.dispose();
});

test("next-turn backpressure is explicit and completed exchanges are not revived", async () => {
  const f = fixture();
  for (let n = 0; n < 16; n++) expect((await f.q.notify(f.root.binding, f.a.binding,
    { request_id: String(n), message: "note" })).ok).toBe(true);
  expect(await f.q.notify(f.root.binding, f.a.binding, { request_id: "overflow", message: "note" }))
    .toEqual({ ok: false, reason: "backpressure" });
  f.q.dispose();
  expect(f.a.queue).toHaveLength(0);
});

test("quick has a marked Unicode display cap and empty/tool-call output is a failure", async () => {
  for (const [text, tools] of [["", false], ["forged", true], ["한".repeat(3000), false]] as const) {
    const f = fixture(), pending = f.ask();
    await f.a.started; f.a.finish(text, tools);
    const result = value(await pending);
    if (!text || tools) expect(result).toMatchObject({ state: "failed", terminal_reason: "empty_reply",
      ended_by: "provider_failure" });
    else {
      expect(Array.from(result.reply!)).toHaveLength(2048);
      expect(Array.from(result.reply!).filter((char) => char === "한").length).toBeLessThan(2048);
    }
    expect(f.a.state.calls).toBe(1);
    f.q.dispose();
  }
});

test("the global four-target limit rejects immediately without a provider queue", async () => {
  const f = fixture(), pending: Promise<Result<AskResult>>[] = [];
  for (const name of ["a", "b", "c", "d"]) {
    const recipient = f.rows.get(name)!;
    pending.push(f.ask(name, { depth: "deep" }, f.root, recipient));
    await recipient.started;
  }
  expect(await f.ask("e", {}, f.root, f.rows.get("e")!)).toEqual({ ok: false, reason: "capacity_exhausted" });
  expect(f.rows.get("e")!.state.calls).toBe(0);
  f.q.dispose();
  expect((await Promise.all(pending)).every((result) => value(result).ended_by === "generation_change")).toBe(true);
  for (const name of ["a", "b", "c", "d"]) f.rows.get(name)!.finish();
});

test("request-local options cap supported wire families and never change the input model", () => {
  for (const alias of ["flash-max", "kimi-max"]) {
    const model = { id: alias, api: "openai-completions", reasoning: true,
      maxTokens: 32000, thinkingLevelMap: { low: "low", max: "max", xhigh: "xhigh" } };
    const before = structuredClone(model);
    expect(value(sideThinkingOptions(model, "max", "quick"))).toMatchObject({
      thinking: "low", outputBudget: 4096, options: { maxTokens: 4096, reasoningEffort: "low" },
    });
    expect(value(sideThinkingOptions(model, "max", "deep"))).toMatchObject({
      thinking: "max", outputBudget: 32000, options: { maxTokens: 32000, reasoningEffort: "max" },
    });
    expect(model).toEqual(before);
  }
  expect(value(sideThinkingOptions({ api: "anthropic-messages", reasoning: true, maxTokens: 32000 },
    "high", "quick")).options).toEqual({ maxTokens: 4096, thinkingEnabled: false });
  expect(sideThinkingOptions({ api: "unknown", reasoning: true, maxTokens: 32000 }, "high", "quick"))
    .toEqual({ ok: false, reason: "reasoning_policy_unsupported" });
  expect(sideThinkingOptions({ api: "openai-responses", reasoning: true, maxTokens: 32000,
    thinkingLevelMap: { low: null } }, "high", "quick").ok).toBe(false);
  const subscription = { api: "claude-sdk-oauth", reasoning: true, maxTokens: 128000,
    thinkingLevelMap: { off: null, minimal: null, low: "low", high: "high", xhigh: "xhigh", max: "max" } };
  expect(value(sideThinkingOptions(subscription, "xhigh", "quick"))).toEqual({
    thinking: "low", outputBudget: 4096, reasoningBudget: null, options: { reasoning: "low" } });
  expect(value(sideThinkingOptions(subscription, "xhigh", "deep"))).toEqual({
    thinking: "xhigh", outputBudget: 128000, reasoningBudget: null, options: { reasoning: "xhigh" } });
  expect(sideThinkingOptions(subscription, "off", "quick")).toEqual({ ok: false, reason: "reasoning_policy_unsupported" });
});

test("Codex quick policy is supported without a fake wire ceiling", () => {
  const model = { id: "gpt-6.1-sol", api: "openai-codex-responses", reasoning: true, maxTokens: 32000,
    thinkingLevelMap: { off: null, minimal: null, low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } };
  const before = structuredClone(model);
  expect(value(sideThinkingOptions(model, "high", "quick"))).toEqual({
    thinking: "low", outputBudget: 4096, reasoningBudget: null, options: { reasoningEffort: "low" },
  });
  expect(value(sideThinkingOptions({ ...model, maxTokens: 2000 }, "high", "quick")).outputBudget).toBe(2000);
  expect(sideThinkingOptions({ ...model, thinkingLevelMap: { ...model.thinkingLevelMap, low: null } },
    "high", "quick")).toEqual({ ok: false, reason: "reasoning_policy_unsupported" });
  expect(value(sideThinkingOptions({ ...model, reasoning: false }, "high", "quick")).options).toEqual({});
  for (const level of ["off", "minimal"]) {
    expect(sideThinkingOptions(model, level, "quick")).toEqual({ ok: false, reason: "reasoning_policy_unsupported" });
  }
  // Deep behaviour is unchanged from the base code captured in task-1/deep-before.txt.
  expect(value(sideThinkingOptions(model, "high", "deep"))).toEqual({
    thinking: "high", outputBudget: 32000, reasoningBudget: null, options: { maxTokens: 32000, reasoningEffort: "high" },
  });
  expect(model).toEqual(before);
});

test("Codex quick verifies low tool-free standard-tier SSE payload after transforms", async () => {
  const model = { id: "gpt-6.1-sol", provider: "chatgpt-subscription", api: "openai-codex-responses",
    reasoning: true, contextWindow: 500000, maxTokens: 32000,
    thinkingLevelMap: { off: null, minimal: null, low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } };
  const mainOptions = { reasoning: "xhigh", streamKind: "main", serviceTier: "priority" };
  const savedModel = structuredClone(model), savedOptions = structuredClone(mainOptions);
  const hostile = { model: "gpt-6.1-sol", input: [{ role: "user", content: "nonce" }],
    reasoning: { effort: "high" }, tools: [{ type: "function", name: "bash" }], tool_choice: "auto",
    max_output_tokens: 99999, max_tokens: 99999, service_tier: "priority" };
  const f = fixture();
  async function runCase(transform: (body: Json) => Json) {
    let streamCalls = 0, options: any;
    let payload: Promise<Json> | undefined;
    const sent: Json[] = [], seenPayloads: Json[] = [];
    const runtime: any = {
      sessionId: "main-session", thinkingLevel: "xhigh", messages: [], _pendingNextTurnMessages: [],
      sessionManager: { getBranch: () => [{ id: "base" }] },
      sendCustomMessage() {},
      agent: { state: { tools: [{ name: "bash", description: "shell", parameters: {} }] },
        convertToLlm: async (messages: unknown[]) => messages },
      extensionRunner: { createContext: () => ({
        prepareProviderRequest: async (messages: unknown[]) => ({ messages,
          transformHeaders: async (headers: unknown) => headers,
          transformPayload: async (body: Json) => transform(body) }),
      }) },
      modelRuntime: {
        getCompatibilityRequestConfig: () => ({}),
        prepareSimpleRequest: async () => ({ model, options: structuredClone(mainOptions) }),
        stream(_model: unknown, _context: unknown, streamOptions: any) {
          streamCalls++; options = streamOptions;
          const first = streamOptions.onPayload(hostile).then((result: Json) => { sent.push(result); return result; });
          payload = first;
          first.catch(() => {});
          return { async *[Symbol.asyncIterator]() {}, result: () => new Promise(() => {}) };
        },
      },
    };
    const capture: any = {
      runtime: () => runtime,
      read: () => ({ ok: true, value: { revision: 1, observed_at: new Date(1000).toISOString(), branch: "base",
        model, system_prompt: "Unit Codex context", messages: [], model_runtime: runtime.modelRuntime } }),
    };
    const adapter = nativeQuestionTarget({ binding: f.a.binding, capture,
      validate: () => ({ ok: true, value: undefined }), subscribe: () => () => {},
      onPayload: (_snapshot, body) => { seenPayloads.push(body); } });
    const prepared = value(await adapter.prepare(value(adapter.read()), "Return nonce", [], "quick",
      new AbortController().signal));
    void prepared.run().catch(() => {});
    if (!payload) throw new Error("payload hook was not invoked");
    let settled: { ok: true; body: Json } | { ok: false; error: unknown };
    try { settled = { ok: true, body: await payload }; } catch (error) { settled = { ok: false, error }; }
    return { prepared, settled, options, sent, seenPayloads, streamCalls, runtime };
  }

  const admitted = await runCase((body) => body);
  if (!admitted.settled.ok) throw admitted.settled.error;
  const body = admitted.settled.body;
  expect(admitted.prepared.thinking).toBe("low");
  expect(admitted.prepared.attempts).toBe(1);
  expect(body.reasoning).toEqual({ effort: "low" });
  for (const key of ["tools", "tool_choice", "max_output_tokens", "max_tokens", "max_completion_tokens", "service_tier"]) {
    expect(Object.hasOwn(body, key)).toBe(false);
  }
  expect(admitted.options).toMatchObject({ transport: "sse", maxRetries: 0 });
  expect(admitted.prepared.controls).toMatchObject({
    wire_output_budget: null, output_cap: "client_abort", output_cap_scope: "visible_text",
    client_output_limit_utf8_bytes: 4096, lane_internal_retry: "possible",
    physical_attempts_basis: "payload_hook", physical_attempts_wire_verified: false,
    wire_reasoning_effort: "low", requested_service_tier: "priority", wire_service_tier: null,
  });
  expect(admitted.seenPayloads).toEqual([body]);
  expect(admitted.sent).toEqual([body]);
  const second = admitted.options.onPayload(hostile) as Promise<Json>;
  expect(await second.then(() => "resolved", (error: Error) => error.message)).toBe("provider_failure");
  expect(admitted.prepared.attempts).toBe(1);
  expect(admitted.runtime.thinkingLevel).toBe("xhigh");
  expect(model).toEqual(savedModel);
  expect(mainOptions).toEqual(savedOptions);

  const refusals: [string, (body: Json) => Json][] = [
    ["transformed thinking.enabled", (candidate) => ({ ...candidate, thinking: { type: "enabled" } })],
    ["oversized reasoning budget", (candidate) => ({ ...candidate, reasoning_budget_tokens: 999999 })],
    ["non-numeric reasoning budget", (candidate) => ({ ...candidate, reasoning_budget_tokens: "many" })],
  ];
  for (const [label, transform] of refusals) {
    const refused = await runCase(transform);
    if (refused.settled.ok) throw new Error(`${label} was admitted`);
    expect(String((refused.settled.error as Error)?.message ?? refused.settled.error)).toBe("reasoning_policy_unsupported");
    expect(refused.prepared.attempts).toBe(0);
    expect(refused.seenPayloads).toHaveLength(0);
    expect(refused.sent).toHaveLength(0);
    expect(refused.streamCalls).toBe(1);
  }

  const overwritten = await runCase((candidate) => ({ ...candidate,
    reasoning: { ...candidate.reasoning, effort: "high" } }));
  if (!overwritten.settled.ok) throw overwritten.settled.error;
  expect(overwritten.settled.body.reasoning).toEqual({ effort: "low" });
  expect(overwritten.prepared.controls.wire_reasoning_effort).toBe("low");
  expect(overwritten.prepared.attempts).toBe(1);
  f.q.dispose();
});

function hasLoneSurrogate(text: string) {
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = text.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) { index++; continue; }
      return true;
    }
    if (code >= 0xdc00 && code <= 0xdfff) return true;
  }
  return false;
}

async function runCodexQuickStream(testCase: { events: readonly Json[]; content: readonly Json[];
  maxTokens?: number; stopReason?: string; caller?: AbortController; usage?: Json }) {
  const f = fixture();
  const model = { id: "gpt-6.1-sol", provider: "chatgpt-subscription", api: "openai-codex-responses",
    reasoning: true, contextWindow: 500000, maxTokens: testCase.maxTokens ?? 32000,
    thinkingLevelMap: { off: null, minimal: null, low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } };
  let seen: { context: Json; options: Json } | undefined, calls = 0, drained = false;
  const runtime: any = {
    sessionId: "main-session", thinkingLevel: "high", messages: [], _pendingNextTurnMessages: [],
    sessionManager: { getBranch: () => [{ id: "base" }] },
    sendCustomMessage() {},
    agent: { state: { tools: [] }, convertToLlm: async (messages: unknown[]) => messages },
    extensionRunner: { createContext: () => ({
      prepareProviderRequest: async (messages: unknown[]) => ({ messages,
        transformHeaders: async (headers: unknown) => headers, transformPayload: async (body: unknown) => body }),
    }) },
    modelRuntime: {
      getCompatibilityRequestConfig: () => ({}),
      prepareSimpleRequest: async () => ({ model, options: {} }),
      stream(_model: unknown, context: Json, streamOptions: Json) {
        calls++; seen = { context, options: streamOptions };
        return {
          async *[Symbol.asyncIterator]() {
            for (const event of testCase.events) yield event;
            drained = true;
          },
          result: () => Promise.resolve({
            stopReason: (streamOptions.signal as AbortSignal).aborted ? "aborted" : testCase.stopReason ?? "stop",
            content: testCase.content,
            usage: testCase.usage ?? { input: 0, output: 0, reasoning: 0, cacheRead: 0, totalTokens: 0 },
          }),
        };
      },
    },
  };
  const capture: any = { runtime: () => runtime, read: () => ({ ok: true, value: {
    revision: 1, observed_at: new Date(1000).toISOString(), branch: "base", model,
    system_prompt: "Unit Codex stream context", messages: [], model_runtime: runtime.modelRuntime } }) };
  const adapter = nativeQuestionTarget({ binding: f.a.binding, capture,
    validate: () => ({ ok: true, value: undefined }), subscribe: () => () => {} });
  const caller = testCase.caller ?? new AbortController();
  const prepared = value(await adapter.prepare(value(adapter.read()), "Return nonce", [], "quick", caller.signal));
  const reply = await prepared.run();
  f.q.dispose();
  if (!seen) throw new Error("stream was not called");
  return { reply, caller, seen, calls, drained: () => drained };
}

test("Codex quick caps streamed UTF8 without corrupting code points", async () => {
  const delta = (contentIndex: number, value: string) => ({ type: "text_delta", contentIndex, delta: value });
  const block = (value: string) => [{ type: "text", text: value }];
  const nullUsage = { input: null, output: null, reasoning: null, cache: null };

  const under = await runCodexQuickStream({ events: [delta(0, "짧은 "), delta(0, "답")], content: block("짧은 답") });
  expect(under.reply).toMatchObject({ text: "짧은 답", completion: "complete" });
  expect(under.caller.signal.aborted).toBe(false);

  // Exactly at the cap with a normal end stays complete.
  const exact = await runCodexQuickStream({ maxTokens: 2000, events: [delta(0, "a".repeat(2000))],
    content: block("a".repeat(2000)) });
  expect(exact.reply).toMatchObject({ completion: "complete", text: "a".repeat(2000) });
  expect(Buffer.byteLength(exact.reply.text)).toBe(2000);
  expect((exact.seen.options.signal as AbortSignal).aborted).toBe(false);

  // Over the cap: keep the longest whole-code-point prefix, drain but ignore later deltas.
  const over = await runCodexQuickStream({ events: [delta(0, "a".repeat(4096)), delta(0, "IGNORED"),
    delta(0, "b")], content: block("a".repeat(4098)) });
  expect(over.reply).toMatchObject({ completion: "truncated_by_client", text: "a".repeat(4096), usage: nullUsage });
  expect((over.seen.options.signal as AbortSignal).aborted).toBe(true);
  expect(over.caller.signal.aborted).toBe(false);
  expect(over.drained()).toBe(true);

  // Korean code points are 3 bytes: 1365 fit, the 1366th would overflow.
  const korean = await runCodexQuickStream({ events: [delta(0, "한".repeat(2000))],
    content: block("한".repeat(2000)) });
  expect(korean.reply.completion).toBe("truncated_by_client");
  expect(korean.reply.text).toBe("한".repeat(1365));
  expect(Buffer.byteLength(korean.reply.text)).toBe(4095);
  expect(hasLoneSurrogate(korean.reply.text)).toBe(false);

  // 4-byte characters: a surrogate pair split across deltas reaches exactly 4096 bytes.
  const split = await runCodexQuickStream({ events: [delta(0, "😀".repeat(1023)), delta(0, "\uD83D"),
    delta(0, "\uDE00")], content: block("😀".repeat(1024)) });
  expect(split.reply).toMatchObject({ completion: "complete", text: "😀".repeat(1024) });
  expect(Buffer.byteLength(split.reply.text)).toBe(4096);
  expect(hasLoneSurrogate(split.reply.text)).toBe(false);

  const emoji = await runCodexQuickStream({ events: [delta(0, "😀".repeat(1025))],
    content: block("😀".repeat(1025)) });
  expect(emoji.reply.completion).toBe("truncated_by_client");
  expect(emoji.reply.text).toBe("😀".repeat(1024));
  expect(Buffer.byteLength(emoji.reply.text)).toBe(4096);
  expect(hasLoneSurrogate(emoji.reply.text)).toBe(false);
  expect(emoji.reply).toMatchObject({ usage: nullUsage });

  // Thinking payloads never count toward the visible-text cap.
  const thinking = await runCodexQuickStream({ events: [
    { type: "thinking_delta", contentIndex: 0, delta: "y".repeat(10000) },
    { type: "thinking_end", contentIndex: 0, content: "x".repeat(10000) },
    delta(0, "ok")], content: block("ok") });
  expect(thinking.reply).toMatchObject({ text: "ok", completion: "complete" });
});

test("Codex quick caps text_end and final-only replies", async () => {
  const delta = (contentIndex: number, value: string) => ({ type: "text_delta", contentIndex, delta: value });
  const end = (contentIndex: number, content: string) => ({ type: "text_end", contentIndex, content });
  const block = (value: string) => [{ type: "text", text: value }];
  const nullUsage = { input: null, output: null, reasoning: null, cache: null };

  // A text_end replaces the block text it supersedes.
  const replaced = await runCodexQuickStream({ events: [delta(0, "wrong"), end(0, "right")], content: block("right") });
  expect(replaced.reply).toMatchObject({ text: "right", completion: "complete" });

  // A text_end larger than the cap with no deltas is cut code-point safely.
  const endOver = await runCodexQuickStream({ events: [end(0, "가".repeat(2000))], content: block("가".repeat(2000)) });
  expect(endOver.reply).toMatchObject({ completion: "truncated_by_client", text: "가".repeat(1365), usage: nullUsage });
  expect((endOver.seen.options.signal as AbortSignal).aborted).toBe(true);

  // Final-only replies with no events at all are capped the same way.
  const finalOnly = await runCodexQuickStream({ events: [], content: block("나".repeat(2000)) });
  expect(finalOnly.reply).toMatchObject({ completion: "truncated_by_client", text: "나".repeat(1365), usage: nullUsage });
  expect(hasLoneSurrogate(finalOnly.reply.text)).toBe(false);

  // Two text blocks are joined with a newline exactly like the normal return path.
  const two = await runCodexQuickStream({ events: [delta(0, "left"), delta(1, "right")],
    content: [{ type: "text", text: "left" }, { type: "text", text: "right" }] });
  expect(two.reply).toMatchObject({ text: "left\nright", completion: "complete" });
});

type Pending<T> = { promise: Promise<T>; resolve(value: T): void; reject(reason: unknown): void };
type CodexStream = (model: Json, context: Json, options: Json) => AsyncIterable<Json> & { result(): Promise<Json> };

const codexQuickModel = (): Json => ({ id: "gpt-6.1-sol", provider: "chatgpt-subscription",
  api: "openai-codex-responses", reasoning: true, contextWindow: 500000, maxTokens: 32000, input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  thinkingLevelMap: { off: null, minimal: null, low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } });

/** A structurally valid JWT carrying the claim the real provider reads; it is never a credential. */
function fakeCodexJwt(): string {
  const encode = (value: Json) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return [encode({ alg: "none", typ: "JWT" }),
    encode({ "https://api.openai.com/auth": { chatgpt_account_id: "acct_test" } }), "signature"].join(".");
}

async function loadCodexProviderStream(): Promise<CodexStream> {
  const entry = "file:///C:/Users/USER/.bun/install/global/node_modules/@earendil-works/pi-ai/dist/api/openai-codex-responses.js";
  const loaded = await import(entry) as { stream: CodexStream };
  return loaded.stream;
}

/** Drives one Codex quick ask through the real ledger and native target with an injected clock. */
function codexAskHarness(stream: CodexStream, options: { apiKey?: string; model?: Json } = {}) {
  let clock = 1000;
  const timers = new Set<{ at: number; callback(): void }>();
  const model = options.model ?? codexQuickModel();
  const streamCalls: Json[] = [], queued: Json[] = [];
  const caller = { kind: "root", target_ref: "coordination:root", owner_root_id: "owner",
    owner_session_id: "owner", canonical_session_id: "root", session_id: "root",
    runtime_instance_id: "root", generation: 1, branch: "base" } as TargetBinding;
  const target = { kind: "worker", target_ref: "coordination:a", owner_root_id: "owner",
    owner_session_id: "owner", canonical_session_id: "a", session_id: "a",
    runtime_instance_id: "a", generation: 1, task_id: "a", run_epoch: 1 } as TargetBinding;
  const runtime: Json = {
    sessionId: "main-session", thinkingLevel: "high", messages: [], _pendingNextTurnMessages: [],
    sessionManager: { getBranch: () => [{ id: "base" }] },
    async sendCustomMessage(message: Json) { queued.push(message); },
    agent: { state: { tools: [] }, convertToLlm: async (messages: unknown[]) => messages },
    extensionRunner: { createContext: () => ({
      prepareProviderRequest: async (messages: unknown[]) => ({ messages,
        transformHeaders: async (headers: unknown) => headers,
        transformPayload: async (body: unknown) => body }),
    }) },
    modelRuntime: {
      getCompatibilityRequestConfig: () => ({}),
      prepareSimpleRequest: async () => ({ model, options: { apiKey: options.apiKey ?? "unit-api-key" } }),
      stream(_streamModel: Json, context: Json, streamOptions: Json) {
        streamCalls.push(streamOptions);
        return stream(model, context, streamOptions);
      },
    },
  };
  const capture: Json = { runtime: () => runtime, read: () => ({ ok: true, value: {
    revision: 1, observed_at: new Date(clock).toISOString(), branch: "base", model,
    system_prompt: "Unit Codex ask context", messages: [], model_runtime: runtime.modelRuntime } }) };
  const adapter = nativeQuestionTarget({ binding: target, capture: capture as any,
    validate: () => ({ ok: true, value: undefined }), subscribe: () => () => {} });
  const callerAdapter = { binding: caller, read: () => ({ ok: true, value: {
      revision: 1, at: new Date(clock).toISOString(), branch: "base", model, thinking: "high",
      context: { messages: [] } } }),
    prepare() { throw new Error("the caller is not a target"); }, async queue() {}, remove() {},
    consumed: () => false, subscribe: () => () => {} } as QuestionTarget;
  const q = createPeerQuestions({ generation: 1, now: () => clock,
    schedule(ms, callback) {
      const timer = { at: clock + ms, callback };
      timers.add(timer);
      return () => { timers.delete(timer); };
    },
    resolve(binding) {
      return binding.target_ref === target.target_ref
        ? { ok: true, value: adapter } : { ok: true, value: callerAdapter };
    } });
  return {
    q, streamCalls, queued, model,
    ask: (id: string, extra: Json = {}) => q.ask(caller, target,
      { target_ref: target.target_ref, question: "Return nonce", request_id: id, ...extra } as any),
    advance(ms: number, fire = true) {
      clock += ms;
      if (fire) for (const timer of [...timers]) if (timer.at <= clock) { timers.delete(timer); timer.callback(); }
    },
    dispose() { q.dispose(); },
  };
}

/** One armed fake provider stream per ask: events, a late reply promise, and its abort signal. */
function codexStreamControl() {
  type Scenario = { events: readonly Json[]; waitForAbort: boolean; reply: Pending<Json>;
    started: Pending<void>; aborted: Pending<void>; signal?: AbortSignal };
  let current: Scenario | undefined;
  return {
    begin(events: readonly Json[], waitForAbort = true): Scenario {
      current = { events, waitForAbort, reply: deferred<Json>(), started: deferred<void>(), aborted: deferred<void>() };
      return current;
    },
    stream: ((_model, _context, streamOptions) => {
      if (!current) throw new Error("no Codex stream scenario was armed");
      const scenario = current;
      const signal = streamOptions.signal as AbortSignal;
      scenario.signal = signal;
      const abort = () => scenario.aborted.resolve();
      if (signal.aborted) abort(); else signal.addEventListener("abort", abort, { once: true });
      scenario.started.resolve();
      return {
        async *[Symbol.asyncIterator]() {
          for (const event of scenario.events) yield event;
          if (scenario.waitForAbort) await scenario.aborted.promise;
        },
        result: () => scenario.reply.promise,
      };
    }) as CodexStream,
  };
}

test("Codex quick cancellation and deadline beat local truncation", async () => {
  const control = codexStreamControl();
  const h = codexAskHarness(control.stream);
  const quiet = () => {
    expect(h.q.diagnostics()).toMatchObject({ active: 0, pending: 0, pending_bytes: 0 });
    expect(h.queued).toHaveLength(0);
  };

  // An already-aborted caller never reaches preparation or the provider.
  const pre = new AbortController();
  pre.abort();
  expect(await h.ask("pre-aborted", { signal: pre.signal })).toEqual({ ok: false, reason: "asker_aborted" });
  expect(h.streamCalls).toHaveLength(0);

  // A caller cancel before any cap wins the terminal.
  const before = new AbortController();
  const beforeState = control.begin([{ type: "text_delta", contentIndex: 0, delta: "under" }]);
  const pendingBefore = h.ask("cancel-before", { signal: before.signal });
  await beforeState.started.promise;
  before.abort();
  await beforeState.aborted.promise;
  expect(beforeState.signal?.aborted).toBe(true);
  beforeState.reply.resolve({ stopReason: "aborted", content: [{ type: "text", text: "under" }],
    usage: { input: 1, output: 1 } });
  const cancelledBefore = value(await pendingBefore);
  expect(cancelledBefore).toMatchObject({ state: "cancelled", ended_by: "asker_aborted" });
  expect(cancelledBefore.completion).toBeUndefined();
  expect(cancelledBefore.reply).toBeUndefined();
  quiet();

  // A caller cancel after the local cap fired still ends as the caller's cancel.
  const after = new AbortController();
  const afterState = control.begin([
    { type: "text_delta", contentIndex: 0, delta: "a".repeat(4096) },
    { type: "text_delta", contentIndex: 0, delta: "overflow" },
  ]);
  const pendingAfter = h.ask("cancel-after", { signal: after.signal });
  await afterState.started.promise;
  await afterState.aborted.promise;
  expect(afterState.signal?.aborted).toBe(true);
  after.abort();
  afterState.reply.resolve({ stopReason: "aborted", content: [{ type: "text", text: "a".repeat(4096) }],
    usage: { input: 1, output: 1 } });
  const cancelledAfter = value(await pendingAfter);
  expect(cancelledAfter).toMatchObject({ state: "cancelled", ended_by: "asker_aborted" });
  expect(cancelledAfter.completion).not.toBe("truncated_by_client");
  expect(cancelledAfter.reply).toBeUndefined();
  quiet();

  // Reasoning-only output never counts toward the cap; the quick deadline ends it.
  const reasoningState = control.begin([
    { type: "thinking_delta", contentIndex: 0, delta: "r".repeat(20000) },
    { type: "thinking_end", contentIndex: 0, content: "r".repeat(20000) },
  ]);
  const reasoning = h.ask("reasoning-deadline", { timeout_ms: 100 });
  await reasoningState.started.promise;
  h.advance(101);
  reasoningState.reply.resolve({ stopReason: "aborted",
    content: [{ type: "thinking", thinking: "r".repeat(20000) }], usage: { input: 1, output: 1 } });
  const timedOut = value(await reasoning);
  expect(timedOut).toMatchObject({ state: "timed_out" });
  expect(timedOut.completion).toBeUndefined();
  expect(timedOut.reply).toBeUndefined();
  quiet();

  // A timer that fires after the absolute deadline cannot admit the late reply.
  const lateState = control.begin([], false);
  const late = h.ask("late-reply", { timeout_ms: 100 });
  await lateState.started.promise;
  h.advance(101, false);
  lateState.reply.resolve({ stopReason: "stop", content: [{ type: "text", text: "late" }],
    usage: { input: 1, output: 1 } });
  const lateResult = value(await late);
  expect(lateResult).toMatchObject({ state: "timed_out" });
  expect(lateResult.completion).toBeUndefined();
  expect(lateResult.reply).toBeUndefined();
  quiet();

  h.dispose();
  expect(h.q.diagnostics()).toMatchObject({ active: 0, pending: 0 });
});

test("Codex quick provider failures do not retry or fall back", async () => {
  const model = codexQuickModel(), apiKey = fakeCodexJwt();
  const provider = await loadCodexProviderStream();
  const originalFetch = globalThis.fetch, originalWebSocket = globalThis.WebSocket;
  const failures: readonly [string, () => Response][] = [
    ["429", () => new Response("rate limited", { status: 429, headers: { "retry-after-ms": "0" } })],
    ["503", () => new Response("service unavailable", { status: 503 })],
    ["network throw", () => { throw new TypeError("unit network failure"); }],
  ];
  try {
    for (const [label, respond] of failures) {
      const requests: { url: string; init: any }[] = [];
      let webSockets = 0;
      globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
        requests.push({ url: String(input), init });
        return respond();
      }) as typeof fetch;
      globalThis.WebSocket = class {
        constructor() { webSockets++; throw new Error("WEBSOCKET_CONSTRUCTED"); }
      } as unknown as typeof WebSocket;
      const h = codexAskHarness((streamModel, context, streamOptions) =>
        provider(streamModel, context, streamOptions), { model, apiKey });
      const result = value(await h.ask(`failure-${label}`));
      expect(result).toMatchObject({ state: "failed", terminal_reason: "provider_failure",
        ended_by: "provider_failure", physical_provider_attempts: 1 });
      expect(result.completion).toBeUndefined();
      expect(requests).toHaveLength(1);
      expect(webSockets).toBe(0);
      expect(requests[0]?.url).toBe("https://chatgpt.com/backend-api/codex/responses");
      expect(requests[0]?.init?.method).toBe("POST");
      expect(new Headers(requests[0]?.init?.headers).get("chatgpt-account-id")).toBe("acct_test");
      expect(h.streamCalls).toHaveLength(1);
      expect(h.streamCalls[0]?.apiKey).toBe(apiKey);
      expect(h.streamCalls[0]).toMatchObject({ transport: "sse", maxRetries: 0 });
      h.dispose();
    }
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.WebSocket = originalWebSocket;
  }
});

test("Codex quick cap holds on the real pi-ai SSE parser", async () => {
  const model = codexQuickModel(), apiKey = fakeCodexJwt();
  const provider = await loadCodexProviderStream();
  const cap = 4096;
  // 1365 Korean code points fill 4095 bytes; the next one cannot fit and must be dropped whole.
  const head = "가".repeat(1365);
  const fullText = `${head}가${"나".repeat(10)}`;
  const events: Json[] = [
    { type: "response.created", response: { id: "resp_unit" } },
    { type: "response.output_item.added", output_index: 0,
      item: { type: "message", id: "msg_unit", role: "assistant", status: "in_progress", content: [] } },
    { type: "response.output_text.delta", output_index: 0, item_id: "msg_unit", content_index: 0, delta: head },
    { type: "response.output_text.delta", output_index: 0, item_id: "msg_unit", content_index: 0, delta: "가" },
    { type: "response.output_text.delta", output_index: 0, item_id: "msg_unit", content_index: 0,
      delta: "나".repeat(10) },
    { type: "response.output_item.done", output_index: 0, item: { type: "message", id: "msg_unit",
      role: "assistant", status: "completed",
      content: [{ type: "output_text", text: fullText, annotations: [] }] } },
    { type: "response.completed", response: { id: "resp_unit", status: "completed", output: [],
      usage: { input_tokens: 11, output_tokens: 22, total_tokens: 33 } } },
  ];
  const sse = events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join("");
  const requests: { url: string; init: any }[] = [];
  const originalFetch = globalThis.fetch, originalWebSocket = globalThis.WebSocket;
  try {
    globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      requests.push({ url: String(input), init });
      return new Response(sse, { status: 200, headers: { "content-type": "text/event-stream" } });
    }) as typeof fetch;
    globalThis.WebSocket = class {
      constructor() { throw new Error("WEBSOCKET_CONSTRUCTED"); }
    } as unknown as typeof WebSocket;
    const h = codexAskHarness((streamModel, context, streamOptions) =>
      provider(streamModel, context, streamOptions), { model, apiKey });
    const result = value(await h.ask("real-sse-cap"));
    expect(result).toMatchObject({ state: "replied", completion: "truncated_by_client",
      physical_provider_attempts: 1,
      usage: { input: null, output: null, reasoning: null, cache: null } });
    const reply = result.reply;
    if (reply === undefined) throw new Error("expected a capped reply");
    expect(reply).toBe(head);
    expect(Buffer.byteLength(reply)).toBeLessThanOrEqual(cap);
    expect(hasLoneSurrogate(reply)).toBe(false);
    expect(requests).toHaveLength(1);
    expect(h.streamCalls).toHaveLength(1);
    h.dispose();
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.WebSocket = originalWebSocket;
  }
});

test("the Claude subscription lane runs one tool-less low-effort side query and caps the visible reply on the stream", async () => {
  const model = { id: "claude-opus-5-5", provider: "anthropic-subscription", api: "claude-sdk-oauth", reasoning: true,
    contextWindow: 600000, maxTokens: 128000,
    thinkingLevelMap: { off: null, minimal: null, low: "low", high: "high", xhigh: "xhigh", max: "max" } };
  for (const [deltas, expected] of [
    [["짧은 ", "답"], { text: "짧은 답", completion: "complete" }],
    [["가".repeat(1000), "나".repeat(1000), "다".repeat(1000)], { completion: "truncated_by_client" }],
  ] as const) {
    const f = fixture();
    let calls = 0, seen: any, streamSignal: AbortSignal | undefined, delivered = 0;
    const runtime: any = {
      sessionId: "main-session", thinkingLevel: "xhigh", messages: [], _pendingNextTurnMessages: [],
      sessionManager: { getBranch: () => [{ id: "base" }] },
      sendCustomMessage() {},
      agent: { state: { tools: [{ name: "bash", description: "shell", parameters: {} }] },
        convertToLlm: async (messages: unknown[]) => messages },
      extensionRunner: { createContext: () => ({
        prepareProviderRequest: async (messages: unknown[]) => ({ messages,
          transformHeaders: async (headers: unknown) => headers, transformPayload: async (body: unknown) => body }),
      }) },
      modelRuntime: {
        getCompatibilityRequestConfig: () => ({}),
        // The main's own request options: the side query must replace reasoning and never inherit streamKind.
        prepareSimpleRequest: async () => ({ model, options: { reasoning: "xhigh", streamKind: "main" } }),
        stream(_model: unknown, context: any, options: any) {
          calls++; seen = { context, options }; streamSignal = options.signal;
          let text = "";
          let finish!: (message: unknown) => void;
          const result = new Promise((resolve) => { finish = resolve; });
          return {
            async *[Symbol.asyncIterator]() {
              for (const delta of deltas) {
                if (options.signal.aborted) break;
                text += delta; delivered++;
                yield { type: "text_delta", delta };
              }
              finish({ stopReason: options.signal.aborted ? "aborted" : "stop",
                content: [{ type: "text", text }], usage: { input: 11, output: 7 } });
            },
            result: () => result,
          };
        },
      },
    };
    const capture: any = {
      runtime: () => runtime,
      read: () => ({ ok: true, value: { revision: 1, observed_at: new Date(1000).toISOString(), branch: "base",
        model, system_prompt: "Unit subscription context", messages: [], model_runtime: runtime.modelRuntime } }),
    };
    const adapter = nativeQuestionTarget({ binding: f.a.binding, capture,
      validate: () => ({ ok: true, value: undefined }), subscribe: () => () => {} });
    const caller = new AbortController();
    const prepared = value(await adapter.prepare(value(adapter.read()), "상태 한 줄로", [], "quick", caller.signal));
    const reply = await prepared.run();
    expect(calls).toBe(1);
    expect(reply.attempts).toBe(1);
    expect(reply.completion).toBe(expected.completion);
    if ("text" in expected) expect(reply.text).toBe(expected.text);
    else {
      expect(Buffer.byteLength(reply.text)).toBeLessThanOrEqual(4096);
      expect(reply.text.startsWith("가".repeat(1000))).toBe(true);
      expect(streamSignal!.aborted).toBe(true);
      expect(delivered).toBe(2);
    }
    expect(caller.signal.aborted).toBe(false);
    expect(seen.options).toMatchObject({ reasoning: "low", toolChoice: "none", maxRetries: 0 });
    expect(seen.options.streamKind).toBeUndefined();
    expect(seen.options.sessionId).toMatch(/^main-session:peer:/);
    expect(seen.context.tools).toEqual([]);
    expect(prepared.thinking).toBe("low");
    expect(prepared.controls).toMatchObject({ output_cap: "client_abort", wire_output_budget: null,
      lane_internal_retry: "possible" });
    expect(runtime.thinkingLevel).toBe("xhigh");
    f.q.dispose();
  }
});

test("final native payload controls override alias extraBody and extension changes without touching main options", async () => {
  for (const [alias, api, responses] of [
    ["responses-unit", "openai-responses", true],
    ["flash-max", "openai-completions", false], ["kimi-max", "openai-completions", false],
    ["anthropic-unit", "anthropic-messages", false],
  ] as const) {
    const f = fixture();
    let providerCalls = 0, payload: any;
    const model = { id: alias, provider: "payload-unit", api, reasoning: true,
      contextWindow: 262144, maxTokens: 32768, thinkingLevelMap: { low: "low", high: "high", max: "max" } };
    const mainOptions = { thinkingBudgets: { low: 8000 }, extraBody: api === "anthropic-messages"
      ? { output_config: { effort: "max" } } : { reasoning_effort: "max" } };
    const saved = structuredClone(mainOptions);
    const runtime: any = {
      sessionId: "a", thinkingLevel: "max", messages: [], _pendingNextTurnMessages: [],
      sessionManager: { getBranch: () => [{ id: "base" }] },
      sendCustomMessage() {},
      agent: { state: { tools: [] }, convertToLlm: async (messages: unknown[]) => messages },
      extensionRunner: { createContext: () => ({
        prepareProviderRequest: async (messages: unknown[]) => ({ messages,
          transformHeaders: async (headers: unknown) => headers,
          transformPayload: async (body: any) => api === "anthropic-messages"
            ? { ...body, output_config: mainOptions.extraBody.output_config, thinking: { type: "enabled" }, max_tokens: 99999 }
            : responses ? { ...body, reasoning: { effort: "max" }, max_output_tokens: 99999 }
              : { ...body, reasoning_effort: "max", max_tokens: 99999 },
        }),
      }) },
      modelRuntime: {
        getCompatibilityRequestConfig: () => mainOptions,
        prepareSimpleRequest: async () => ({ model, options: structuredClone(mainOptions) }),
        stream(_model: unknown, _context: unknown, options: any) {
          return { async result() {
            payload = await options.onPayload(api === "anthropic-messages"
              ? { model: alias, thinking: { type: "enabled" }, max_tokens: 99999 }
              : responses ? { model: alias, reasoning: { effort: "max" }, max_output_tokens: 99999 }
                : { model: alias, reasoning_effort: "max", max_tokens: 99999 });
            providerCalls++;
            return { stopReason: "stop", content: [{ type: "text", text: "payload-nonce" }], usage: { input: 1, output: 1 } };
          } };
        },
      },
    };
    const capture: any = {
      runtime: () => runtime,
      read: () => ({ ok: true, value: { revision: 1, observed_at: new Date(1000).toISOString(), branch: "base",
        model, system_prompt: "Unit payload context", messages: [], model_runtime: runtime.modelRuntime } }),
    };
    const adapter = nativeQuestionTarget({ binding: f.a.binding, capture,
      validate: () => ({ ok: true, value: undefined }), subscribe: () => () => {} });
    const prepared = value(await adapter.prepare(value(adapter.read()), "Return payload-nonce", [], "quick", new AbortController().signal));
    expect((await prepared.run()).text).toBe("payload-nonce");
    expect(providerCalls).toBe(1);
    expect(responses ? payload.max_output_tokens : payload.max_tokens).toBe(4096);
    if (api === "anthropic-messages") {
      expect(payload.thinking.type).toBe("disabled");
      expect(payload.output_config.effort).toBeUndefined();
    } else expect(responses ? payload.reasoning.effort : payload.reasoning_effort).toBe("low");
    expect(prepared.controls.wire_output_budget).toBe(4096);
    expect(runtime.thinkingLevel).toBe("max");
    expect(mainOptions).toEqual(saved);
    f.q.dispose();
  }
});

test("shutdown cancels an event-gated child bootstrap and releases its watcher and capture listener", async () => {
  const directory = fs.mkdtempSync(path.join(questionEvidence, "bootstrap-abort-"));
  const agentDir = path.join(directory, "agent");
  fs.mkdirSync(path.join(agentDir, "agent-coordination/bootstrap"), { recursive: true });
  const subscribed = deferred<void>(), closed = deferred<void>();
  const watch = fs.watch;
  let listeners = 0;
  fs.watch = ((...args: Parameters<typeof fs.watch>) => {
    const watcher = Reflect.apply(watch, fs, args);
    watcher.once("close", () => closed.resolve());
    subscribed.resolve();
    return watcher;
  }) as typeof fs.watch;
  const controller = new AbortController();
  const previousDirectory = process.env.SENPI_CODING_AGENT_SESSION_DIR;
  process.env.SENPI_CODING_AGENT_SESSION_DIR = path.join(directory, "sessions/st_bootstrapabort");
  try {
    const pending = connectManagedQuestionChild({
      context: { agentDir, sessionManager: {} }, capture: {} as any,
      captureStore: { onRuntime() { listeners++; return () => { listeners--; }; } } as any,
      signal: controller.signal,
    });
    await subscribed.promise;
    controller.abort();
    expect(await pending).toEqual({ ok: false, reason: "stale_generation" });
    await closed.promise;
    expect(listeners).toBe(0);
    expect(fs.readdirSync(path.join(agentDir, "agent-coordination/bootstrap"))).toEqual([]);
  } finally {
    if (previousDirectory === undefined) delete process.env.SENPI_CODING_AGENT_SESSION_DIR;
    else process.env.SENPI_CODING_AGENT_SESSION_DIR = previousDirectory;
    fs.watch = watch; fs.rmSync(directory, { recursive: true });
  }
});

test("ordinary managed RPC children bootstrap in the product and share tool/API deduplication in both directions", async () => {
  const report = await runQuestionCase("question-process");
  expect(report.passed).toBe(true);
  expect(report.process_connection).toMatchObject({
    actual_named_pipe_identity_roundtrip: true, side_provider_calls: 1,
    production_bootstrap: true, fixture_handshake_calls: 0,
    tool_api_shared_ledger: true, native_main_completed: true,
    registered_question_result: { ok: true, value: { state: "replied", physical_provider_attempts: 1 } },
    outbound: { child_to_root_tool_api_deduplicated: true, copied_id_context_refused: true,
      child_to_root: { ok: true, value: { physical_provider_attempts: 1 } },
      child_to_sibling: { ok: true, value: { physical_provider_attempts: 1 } } },
  });
}, 120_000);

test("registered tool and transport cached requests revalidate current ask policy without provider replay", async () => {
  const report = await runQuestionCase("ask-policy");
  expect(report.passed).toBe(true);
  expect(report.policy_replay).toMatchObject({
    first: { ok: true, value: { state: "replied", reply: "policy-replay" } },
    asks_while_denied: "denied",
    registered_ask: { ok: false, reason: "target_excluded" },
    transport_ask: { ok: false, reason: "target_excluded" },
    api_ask_and_notify_denied: true,
    provider_calls_during_replay: 0, queue_additions_during_replay: 0,
    allow_from_cached_result_preserved: true, default_cached_result_preserved: true,
  });
}, 60_000);

test("a replayed request_id from a child session shows no second set of asker lines", async () => {
  const sessionManager = {}, lines: string[] = [], cache = new Map<string, unknown>();
  let sent = 0;
  const ctx = { sessionManager, ui: { notify: (message: string) => { lines.push(message); } } };
  const capture = { runtime: () => ({ sessionManager }) } as any;
  const captureStore = { forRuntime: () => ({ ok: true, value: capture }), onRuntime: () => () => {} } as any;
  const connection = {
    binding: { kind: "worker", target_ref: "coordination:a", task_id: "a" }, root_ref: "coordination:root",
    isCurrent: () => true, onClose: () => () => {}, dispose() {},
    async request(request: any) {
      if (request.operation === "coordination.question.peers") return { ok: true, value: { revision: 1, peers: [
        { target_ref: "coordination:root", name: "root", role: "root" }, { target_ref: "coordination:a", name: "a", role: "explore" }] } };
      // Owner-side channel ledger: one execution per request_id, replays served from cache.
      if (!cache.has(request.request_id)) {
        sent++;
        cache.set(request.request_id, request.operation === "ask"
          ? { ok: true, value: { state: "replied", reply: "answer", latency_ms: 3000 } }
          : { ok: true, value: { state: "delivered", incorporation: "queued_for_next_turn" } });
      }
      return cache.get(request.request_id);
    },
  } as any;
  const created = await createRemoteQuestionOwner({ context: ctx, capture, captureStore, connection });
  if (!created.ok) throw new Error(created.reason);
  const owner = created.value.owner;
  const ask = { target_ref: "coordination:root", request_id: "q-1", question: "status?" };
  expect(await owner.request(ctx, "ask", ask)).toEqual(await owner.request(ctx, "ask", ask));
  expect(lines).toEqual(["▸ 협업  a → root  \"status?\"  ⏳ 대기", "▸ 협업  root → a  \"answer\"  ✅ 3s (quick)"]);
  const note = { target_ref: "coordination:root", request_id: "n-1", message: "fyi" };
  await owner.request(ctx, "notify", note); await owner.request(ctx, "notify", note);
  expect(lines.slice(2)).toEqual(["▸ 협업  a → root  노트 전달(중단 없음)  📝 다음 턴"]);
  expect(sent).toBe(2);
  created.value.dispose();
});

test("a child request refused before owner acceptance still shows lines when the same request_id later executes", async () => {
  const sessionManager = {}, lines: string[] = [], cache = new Map<string, unknown>();
  let sent = 0, denyOnce = true;
  const ctx = { sessionManager, ui: { notify: (message: string) => { lines.push(message); } } };
  const capture = { runtime: () => ({ sessionManager }) } as any;
  const captureStore = { forRuntime: () => ({ ok: true, value: capture }), onRuntime: () => () => {} } as any;
  const connection = {
    binding: { kind: "worker", target_ref: "coordination:a", task_id: "a" }, root_ref: "coordination:root",
    isCurrent: () => true, onClose: () => () => {}, dispose() {},
    async request(request: any) {
      if (request.operation === "coordination.question.peers") return { ok: true, value: { revision: 1, peers: [
        { target_ref: "coordination:root", name: "root", role: "root" }, { target_ref: "coordination:a", name: "a", role: "explore" }] } };
      // Channel pre-ledger refusals (validation/authorization) are not cached.
      if (request.target_ref === "coordination:bad") return { ok: false, reason: "invalid_target" };
      if (request.request_id === "q-auth" && denyOnce) { denyOnce = false; return { ok: false, reason: "target_excluded" }; }
      if (!cache.has(request.request_id)) {
        sent++;
        cache.set(request.request_id, request.request_id === "q-busy" ? { ok: false, reason: "target_busy" }
          : { ok: true, value: { state: "replied", reply: "answer", latency_ms: 3000 } });
      }
      return cache.get(request.request_id);
    },
  } as any;
  const created = await createRemoteQuestionOwner({ context: ctx, capture, captureStore, connection });
  if (!created.ok) throw new Error(created.reason);
  const owner = created.value.owner;
  const ask = (request_id: string, target_ref = "coordination:root") => owner.request(ctx, "ask", { target_ref, request_id, question: "status?" });
  const success = ["▸ 협업  a → root  \"status?\"  ⏳ 대기", "▸ 협업  root → a  \"answer\"  ✅ 3s (quick)"];
  await ask("q-x", "coordination:bad");
  expect(lines).toEqual(["▸ 협업  a → coordination:bad  \"status?\"  ⏳ 대기", "▸ 협업  coordination:bad → a  invalid_target  ❌ <1s (quick)"]);
  expect((await ask("q-x")).ok).toBe(true);
  expect(lines.slice(2)).toEqual(success);
  await ask("q-x");
  expect(lines).toHaveLength(4);
  await ask("q-auth"); await ask("q-auth");
  expect(lines.slice(4)).toEqual(["▸ 협업  a → root  \"status?\"  ⏳ 대기", "▸ 협업  root → a  target_excluded  ❌ <1s (quick)", ...success]);
  await ask("q-busy"); await ask("q-busy");
  expect(lines.slice(8)).toEqual(["▸ 협업  a → root  \"status?\"  ⏳ 대기", "▸ 협업  root → a  target_busy  ❌ <1s (quick)"]);
  expect(sent).toBe(3);
  created.value.dispose();
});
