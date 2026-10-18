import { connectEngineBridge } from "./engine-bridge.ts";
import type { SenpiNamespace } from "./engine-bridge.ts";

export const CARRIER_MISSING_WARNING = "⚠ 협업 모드: 캐리어 없음 — 코디 도구 사용 불가";

interface WarnContext { sessionManager: object; ui?: { notify(message: string, kind: "warning" | "info"): void } }

const warned = new WeakSet<object>();

/** Failure-only session-start notice. A throwaway session manager makes the bridge
 * validate the carrier and accepted baseline without binding anything, so only
 * "native adapter carrier" means absent/mismatched; mode_disabled stays silent. */
export function warnIfCarrierMissing(context: WarnContext, loaderNamespace: SenpiNamespace, generation: number): boolean {
  if (warned.has(context.sessionManager)) return false;
  const probe = connectEngineBridge({
    context: { sessionManager: { getSessionId: () => "" } }, generation, loaderNamespace,
  });
  if (probe.ok || probe.connection !== "native adapter carrier") return false;
  warned.add(context.sessionManager);
  try { context.ui?.notify(CARRIER_MISSING_WARNING, "warning"); } catch { return false; }
  return true;
}
