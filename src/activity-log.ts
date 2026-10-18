import fs from "node:fs";
import path from "node:path";

/** Activity lines follow omo-agent-coordination/activity-line/1 (agreed with the
 * dashboard reader). The file lives in the agent dir this mod already resolves
 * (context.agentDir), so an isolated agent dir redirects it. Writes are
 * best-effort: one appendFileSync per event, every I/O error is dropped, and the
 * file is rotated to `.1` once it exceeds 1 MiB before an append. */
export const ACTIVITY_FILE = "coordination-activity.jsonl";
export const ACTIVITY_MAX_BYTES = 1024 * 1024;

export type ActivityKind = "ask" | "notify" | "inspect" | "repair" | "advisor" | "status";
export interface ActivityEvent {
  readonly kind: ActivityKind;
  readonly from: string;
  readonly to: string | null;
  readonly summary: string;
  readonly ok: boolean | null;
  readonly phase?: "request" | "reply" | "done";
  readonly id?: string;
  readonly latency_ms?: number;
  readonly mode?: string;
  readonly delivery?: string;
  readonly session?: string;
  readonly error?: string;
  /** Event time when it differs from the append time (buffered events). Not written as a field. */
  readonly at?: string;
}
interface UiLike { notify(message: string, kind: "info"): void }

/** Single line, control characters folded to spaces, at most `max` code points. */
export function shorten(text: unknown, max: number): string {
  const flat = Array.from(String(text ?? "").replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]+/g, " ")
    .replace(/\s+/g, " ").trim());
  return flat.length > max ? flat.slice(0, max - 1).join("") + "…" : flat.join("");
}
export const activityLabel = (text: unknown) => shorten(text, 40) || "?";
export const activityPath = (agentDir: string) => path.join(path.resolve(agentDir), ACTIVITY_FILE);

export function activityLine(event: ActivityEvent, now = new Date()) {
  return {
    v: 1, ts: now.toISOString(), kind: event.kind, from: activityLabel(event.from),
    to: event.to === null ? null : activityLabel(event.to), summary: shorten(event.summary, 160), ok: event.ok,
    ...(event.phase ? { phase: event.phase } : {}),
    ...(event.id ? { id: event.id } : {}),
    ...(event.latency_ms !== undefined ? { latency_ms: Math.max(0, Math.round(event.latency_ms)) } : {}),
    ...(event.mode ? { mode: shorten(event.mode, 16) } : {}),
    ...(event.delivery ? { delivery: shorten(event.delivery, 16) } : {}),
    ...(event.session ? { session: event.session } : {}),
    ...(event.error ? { error: shorten(event.error, 120) } : {}),
  };
}

export function appendActivity(agentDir: string | undefined, event: ActivityEvent): boolean {
  try {
    if (typeof agentDir !== "string" || !agentDir) return false;
    const file = activityPath(agentDir);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    try {
      if (fs.statSync(file).size > ACTIVITY_MAX_BYTES) fs.renameSync(file, file + ".1");
    } catch { /* missing file or a concurrent rotation: append below starts/continues a file */ }
    fs.appendFileSync(file, JSON.stringify(activityLine(event, event.at ? new Date(event.at) : new Date())) + "\n");
    return true;
  } catch { return false; }
}

/** Producers without a session context (auto-repair holds only the EngineBridge)
 * log against an owner object. The owner's agent dir is bound once the session's
 * collaboration owner exists; events raised earlier (e.g. the attach-time repair)
 * are buffered and flushed then. An owner that is never bound writes nothing. */
const ownerDirs = new WeakMap<object, string>();
const ownerPending = new WeakMap<object, ActivityEvent[]>();
export function bindActivityDir(owner: object, agentDir: string) {
  ownerDirs.set(owner, agentDir);
  for (const event of ownerPending.get(owner) ?? []) appendActivity(agentDir, event);
  ownerPending.delete(owner);
}
export function appendOwnerActivity(owner: object, event: ActivityEvent) {
  const dir = ownerDirs.get(owner);
  if (dir !== undefined) return appendActivity(dir, event);
  const pending = ownerPending.get(owner) ?? [];
  if (pending.length < 32) pending.push({ ...event, at: event.at ?? new Date().toISOString() });
  ownerPending.set(owner, pending);
  return false;
}

const seconds = (ms: number) => ms < 1000 ? "<1s" : `${Math.round(ms / 1000)}s`;
const quoted = (text: unknown) => `"${shorten(text, 60)}"`;
/** `▸ 협업` session lines, shaped after the confirmed v4 mockup. */
export const collabLine = {
  askSent: (from: string, to: string, question: unknown) =>
    `▸ 협업  ${activityLabel(from)} → ${activityLabel(to)}  ${quoted(question)}  ⏳ 대기`,
  askReply: (from: string, to: string, ok: boolean, text: unknown, ms: number, mode: string) =>
    `▸ 협업  ${activityLabel(from)} → ${activityLabel(to)}  ${ok ? quoted(text) : shorten(text, 60)}  ` +
    `${ok ? "✅" : "❌"} ${seconds(ms)} (${shorten(mode, 16)})`,
  asked: (from: string, question: unknown, mode: string) =>
    `▸ 협업  ${activityLabel(from)}가 물음 — ${quoted(question)} (${shorten(mode, 16)}, 답 1회)`,
  replied: (to: string, text: unknown) => `▸ 협업  답 보냄 → ${activityLabel(to)}  ${quoted(text)}  ✅`,
  notified: (from: string, to: string, ok: boolean, reason?: unknown) =>
    `▸ 협업  ${activityLabel(from)} → ${activityLabel(to)}  ` +
    (ok ? "노트 전달(중단 없음)  📝 다음 턴" : `노트 실패  ❌ ${shorten(reason, 40)}`),
};

/** Best-effort display through the session's own ui.notify; never throws. */
export function showCollab(ui: UiLike | undefined, line: string): boolean {
  try { if (!ui || typeof ui.notify !== "function") return false; ui.notify(line, "info"); return true; }
  catch { return false; }
}
