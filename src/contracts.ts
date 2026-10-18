export const REQUEST_STATES = Object.freeze([
  "accepted", "delivered", "replied", "failed", "timed_out", "cancelled",
] as const);
export type RequestState = typeof REQUEST_STATES[number];
export const TERMINAL_STATES = Object.freeze(["replied", "failed", "timed_out", "cancelled"] as const);
export const TERMINAL_REASONS = Object.freeze([
  "target_not_live", "target_settled", "stale_epoch", "scope_denied", "target_excluded",
  "target_busy", "capacity_exhausted", "invalid_argument", "invalid_target", "policy_not_owner",
  "asker_aborted", "unsupported_runtime", "context_unavailable", "context_too_large",
  "provider_failure", "empty_reply", "reasoning_policy_unsupported", "owner_cancel",
  "generation_change", "host_failure", "conflict", "coordination_unavailable",
  "api_version_unsupported", "stale_generation", "backpressure",
] as const);
export type TerminalReason = typeof TERMINAL_REASONS[number];
export const DEPTHS = Object.freeze(["quick", "deep"] as const);
export type Depth = typeof DEPTHS[number];
export const ENDED_BY = Object.freeze([
  "reply", "target_settled", "target_not_live", "asker_aborted", "owner_cancel",
  "generation_change", "provider_failure", "host_failure",
] as const);
export type EndedBy = typeof ENDED_BY[number];
export const FEATURES = Object.freeze([
  "ask_depth_deep", "ask_policy", "inspect", "revival_settlement_repair",
] as const);
export type Feature = typeof FEATURES[number];
export const THINKING_POLICIES = Object.freeze(["low-cap-v1", "target-effective-v1"] as const);
export type SideThinkingPolicy = typeof THINKING_POLICIES[number];
export const NATIVE_STATES = Object.freeze([
  "pending", "running", "completed", "error", "cancelled", "interrupted", "lost",
] as const);
export type NativeState = typeof NATIVE_STATES[number];
export const RESIDENCY_STATES = Object.freeze([
  "resident", "persisted_only", "rpc_detached", "evicted", "disposed",
] as const);
export const INCORPORATION_STATES = Object.freeze([
  "queued_for_next_turn", "incorporated", "dropped", "not_incorporated",
] as const);
export type IncorporationState = typeof INCORPORATION_STATES[number];
export const ASK_POLICIES = Object.freeze(["default", "deny_peer_asks"] as const);
export type AskPolicy = typeof ASK_POLICIES[number];
export const ASKS = Object.freeze(["allowed", "denied"] as const);
export type Asks = typeof ASKS[number];
export const DENIAL_REASONS = Object.freeze([
  "one_shot", "independent_review", "assignment_denied", "target_excluded",
] as const);
export type DenialReason = typeof DENIAL_REASONS[number];
export const INSPECT_FIELDS = Object.freeze([
  "live", "status", "run_epoch", "last_transition_at", "record_consistent", "asks",
] as const);
export const API_PORTS = Object.freeze([
  "peers", "ask", "notify", "inspect", "setAskPolicy", "onPeersChanged", "dispose",
] as const);
export const LIMITS = Object.freeze({
  question_utf8_bytes: 16 * 1024, message_utf8_bytes: 4 * 1024,
  request_frame_utf8_bytes: 64 * 1024, quick_timeout_default_ms: 30_000,
  quick_timeout_max_ms: 60_000, target_concurrency: 1, global_concurrency: 4,
  quick_output_tokens: 4096, quick_reasoning_tokens: 2048, quick_display_characters: 2048,
  pending_target_exchanges: 16, pending_global_exchanges: 64, pending_utf8_bytes: 256 * 1024,
});

declare const targetRefBrand: unique symbol;
/** Opaque broker-issued address; possession alone grants no authority. */
export type TargetRef = string & { readonly [targetRefBrand]: true };
export type TargetBinding = Readonly<{
  target_ref: TargetRef;
  owner_session_id: string;
  owner_root_id: string;
  canonical_session_id: string;
  session_id: string;
  runtime_instance_id: string;
  generation: number;
} & (
  { kind: "worker"; task_id: string; run_epoch: number } |
  { kind: "root"; branch: string }
)>;
export interface Fact<T> {
  readonly value: T | "unknown";
  readonly source: string;
  readonly observed_at: string;
  readonly evidence_ref?: string;
}
export type RecordConsistent = true | false | "unknown";
export interface InspectProjection {
  readonly live: boolean | "unknown";
  readonly status: NativeState | "unknown";
  readonly run_epoch: number | null;
  readonly last_transition_at: string | null;
  readonly record_consistent: RecordConsistent;
  readonly asks: Asks;
}
export interface TargetInspection extends InspectProjection {
  readonly target_ref: TargetRef;
  readonly recorded_state: Fact<NativeState>;
  readonly runtime_reachability: Fact<boolean>;
  readonly activity: Fact<string>;
  readonly progress: Fact<{ evidence_ref: string; at: string }>;
  readonly work_outcome: Fact<string>;
  readonly publication: Fact<string>;
  readonly notification: Fact<string>;
  readonly slot: Fact<string>;
  readonly residency: Fact<typeof RESIDENCY_STATES[number]>;
  readonly diagnostic_class: "queued" | "never_started" | "live_blocked_tool" |
    "external_wait" | "transport_unreachable" | "completed_unpublished" | "normal" | "unknown";
}
export interface Peer {
  readonly target_ref: TargetRef;
  readonly name: string;
  readonly role: string;
  readonly ask_capability: boolean;
  readonly notify_capability: boolean;
  readonly asks: Asks;
  readonly denial_reason?: DenialReason;
}
export interface Roster {
  readonly revision: number;
  readonly peers: readonly Peer[];
}
export interface TreeInspection extends Roster {
  readonly owned: number;
  readonly visited: number;
  readonly unreadable: number;
  readonly coverage: "complete" | "partial" | "unknown";
  readonly targets: readonly TargetInspection[];
}
export type AskInput = Readonly<{
  target_ref: TargetRef;
  question: string;
  request_id: string;
  signal?: AbortSignal;
} & (
  { depth?: "quick"; timeout_ms?: number } |
  { depth: "deep"; timeout_ms?: never }
)>;
export type ToolAskInput = (
  Omit<Extract<AskInput, { depth?: "quick" }>, "request_id"> |
  Omit<Extract<AskInput, { depth: "deep" }>, "request_id">
) & { readonly request_id?: string };
export interface NotifyInput {
  readonly target_ref: TargetRef;
  readonly message: string;
  readonly request_id: string;
}
export interface Usage {
  readonly input: number | null;
  readonly output: number | null;
  readonly reasoning: number | null;
  readonly cache: number | null;
}
export interface RequestReceipt {
  readonly request_id: string;
  readonly state: RequestState;
  readonly terminal_reason?: TerminalReason;
  readonly target_ref: TargetRef;
  readonly run_epoch: number | null;
  readonly generation: number;
  readonly as_of_revision: number | null;
  readonly as_of_time: string | null;
  readonly current_revision: number | null;
  readonly incorporation: IncorporationState;
  readonly observed_at: string;
}
export interface AskResult extends RequestReceipt {
  readonly depth: Depth;
  readonly reply?: string;
  readonly completion?: "complete" | "truncated_by_provider";
  readonly ended_by?: EndedBy;
  readonly side_thinking_policy: SideThinkingPolicy;
  readonly target_thinking_before: string;
  readonly effective_side_thinking: string;
  readonly effective_side_reasoning_budget: number | null;
  readonly effective_side_output_budget: number;
  readonly main_thinking_unchanged: boolean;
  readonly target_model: string;
  readonly target_route: string;
  readonly latency_ms: number;
  readonly usage: Usage;
  readonly logical_provider_requests: number;
  readonly physical_provider_attempts: number;
  /** Only thinking/output controls; never auth, prompts, or transcript data. */
  readonly provider_controls: Readonly<Record<string, string | number | boolean | null>>;
}
export type Refusal = Readonly<{ ok: false; reason: TerminalReason }>;
export type Result<T> = Readonly<{ ok: true; value: T }> | Refusal;
export interface AskPolicyInput {
  readonly target_ref: TargetRef;
  readonly policy: AskPolicy;
  readonly allow_from?: readonly TargetRef[];
}
export interface CoordinationClient {
  peers(): Result<Roster>;
  ask(input: AskInput): Promise<Result<AskResult>>;
  notify(input: NotifyInput): Promise<Result<RequestReceipt>>;
  inspect(target_ref: TargetRef): Promise<Result<InspectProjection>>;
  setAskPolicy(input: AskPolicyInput): Result<{ asks: Asks }>;
  onPeersChanged(listener: (roster: Roster) => void): () => void;
  dispose(): void;
}
export interface CoordinationBroker {
  readonly version: 1;
  readonly features: readonly Feature[];
  getClient(ctx: unknown): Result<CoordinationClient>;
}

export function isRequestState(value: unknown): value is RequestState {
  return typeof value === "string" && REQUEST_STATES.some((state) => state === value);
}
export function isRecordConsistent(value: unknown): value is RecordConsistent {
  return value === true || value === false || value === "unknown";
}
export function isTargetBinding(value: unknown): value is TargetBinding {
  if (typeof value !== "object" || value === null) return false;
  const binding = value as Record<string, unknown>;
  if (!["target_ref", "owner_session_id", "owner_root_id", "canonical_session_id",
    "session_id", "runtime_instance_id"].every((key) =>
    typeof binding[key] === "string" && binding[key].length > 0)) return false;
  if (!Number.isSafeInteger(binding.generation) || Number(binding.generation) < 0) return false;
  return binding.kind === "worker"
    ? typeof binding.task_id === "string" && binding.task_id.length > 0 &&
      Number.isSafeInteger(binding.run_epoch) && Number(binding.run_epoch) >= 0
    : binding.kind === "root" && typeof binding.branch === "string" && binding.branch.length > 0;
}
