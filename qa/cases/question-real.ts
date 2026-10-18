import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { sideThinkingOptions } from "../../src/peer-questions.ts";
import { questionEvidence, questionHashes } from "./question-receipts.ts";

export function inspectQuestionModels(registry: any) {
  const preferred = ["opencode-go", "kimi-coding", "chatgpt-subscription"];
  return registry.getAll().filter((model: any) => !model.provider.startsWith("coordination-") && model.reasoning &&
    (preferred.includes(model.provider) || registry.hasConfiguredAuth(model)))
    .map((model: any) => ({ provider: model.provider, id: model.id, api: model.api,
      context_window: model.contextWindow, native_max_tokens: model.maxTokens,
      alias: model.provider === "opencode-go" && /deepseek.*flash/.test(model.id) ? "flash-max" :
        model.provider === "kimi-coding" ? "kimi-max" : model.id,
      auth_configured: registry.hasConfiguredAuth(model),
      quick_supported: sideThinkingOptions(model, "high", "quick").ok,
      deep_supported: sideThinkingOptions(model, "high", "deep").ok }))
    .sort((left: any, right: any) => {
      const rank = (provider: string) => preferred.includes(provider) ? preferred.indexOf(provider) : preferred.length;
      const modelRank = (model: any) => /deepseek.*flash/.test(model.id) || model.id === "gpt-6.1-sol" ? 0 : 1;
      return rank(left.provider) - rank(right.provider) || modelRank(left) - modelRank(right);
    });
}

/** Exactly one persisted request admission per named paid case, including failed
 * attempts. Availability selection happens before this admission, never after it. */
export async function realQuestion(options: {
  name: "max-reasoning-nonce" | "deep-ask-long"; root: any; registry: any; rootRef: string;
  childContext: any; toolAsk: (...args: any[]) => Promise<any>;
}) {
  const { name, root, registry } = options;
  const nativeProfile = "C:/Users/USER/.omo-stable-500/home/.omo/agent";
  const inspected = inspectQuestionModels(registry);
  const eligible = inspected.find((candidate: any) => candidate.auth_configured &&
    candidate.provider === "ollama" && candidate.id === "deepseek-v4.1-flash" &&
    (name === "deep-ask-long" ? candidate.deep_supported : candidate.quick_supported));
  const selected = eligible && { ...eligible, model: registry.find(eligible.provider, eligible.id) };
  if (!selected) return { passed: false, unavailable: true, reason: "NO_CONFIGURED_SUPPORTED_HIGH_THINKING_TARGET",
    candidates: inspected,
    credential_path: nativeProfile + "/auth.json", credential_access: "native ReadOnlyAuthStorage; no credential files copied" };
  const originalModel = root.model, originalThinking = root.thinkingLevel;
  const guard = path.join(questionEvidence, `${name}-single-request.json`);
  const fd = fs.openSync(guard, "wx");
  const nonce = randomUUID().replaceAll("-", "");
  const claim = { case: name, request_id: `${name}:${nonce}`, source_sha256: questionHashes(),
    selected: { provider: selected.provider, id: selected.id, alias: selected.alias, api: selected.model.api },
    admitted_at: new Date().toISOString(), credentials_copied: false,
    credential_path: nativeProfile + "/auth.json", credential_access: "native ReadOnlyAuthStorage, read-only",
    app_retries: 0, fallback: 0, logical_request_limit: 1 };
  fs.writeFileSync(fd, JSON.stringify(claim, null, 2) + "\n"); fs.closeSync(fd);
  const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
  try {
    assert(await root.setModel(selected.model), "REAL_TARGET_MODEL_NOT_REACHABLE");
    root.setSessionThinkingLevel("high");
    assert(["high", "xhigh", "max"].includes(root.thinkingLevel), "REAL_TARGET_HIGH_THINKING_UNAVAILABLE");
    const before = { model: hash(root.model), thinking: root.thinkingLevel, history: hash(root.messages) };
    const question = name === "max-reasoning-nonce"
      ? `Reply with exactly this nonce in plain text: ${nonce}.`
      : `Begin with the exact nonce ${nonce}. Then write an original explanation of event-driven software concurrency in at least 80 numbered sentences. Each sentence must be at least 70 characters long. Do not use tools. Complete the explanation without abbreviating it.`;
    const result = await options.toolAsk(options.rootRef, claim.request_id, question,
      name === "max-reasoning-nonce" ? { timeout_ms: 60000 } : { depth: "deep" }, options.childContext);
    assert(result.ok, JSON.stringify(result));
    const receipt = result.value;
    assert.equal(receipt.state, "replied", JSON.stringify(receipt));
    assert(receipt.reply?.includes(nonce), "REAL_MODEL_NONCE_NOT_OBSERVED");
    assert.equal(receipt.logical_provider_requests, 1);
    assert.equal(receipt.physical_provider_attempts, 1, "REAL_PROVIDER_SINGLE_ATTEMPT_NOT_PROVED");
    assert.equal(hash(root.model), before.model);
    assert.equal(root.thinkingLevel, before.thinking);
    assert.equal(hash(root.messages), before.history);
    if (name === "deep-ask-long") {
      assert(Array.from(receipt.reply).length > 2048, "REAL_DEEP_REPLY_NOT_LONG");
      assert.equal(receipt.completion, "complete", "REAL_DEEP_PROVIDER_TRUNCATION");
      assert.equal(receipt.effective_side_thinking, before.thinking);
      assert.equal(receipt.effective_side_output_budget, selected.model.maxTokens);
    } else {
      assert(["off", "minimal", "low"].includes(receipt.effective_side_thinking));
      assert(receipt.effective_side_output_budget <= 4096);
    }
    return { ...claim, passed: true, unavailable: false, receipt, target_thinking_before: before.thinking,
      main_thinking_unchanged: true, main_model_unchanged: true, immediate_history_unchanged: true,
      reply_characters: Array.from(receipt.reply).length };
  } finally {
    await root.setModel(originalModel);
    root.setSessionThinkingLevel(originalThinking);
  }
}
