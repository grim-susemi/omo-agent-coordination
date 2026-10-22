import { createRuntimeCapture } from "./runtime-capture.ts";
import type { RuntimeCapture } from "./runtime-capture.ts";
import { connectEngineBridge } from "./engine-bridge.ts";
import { warnIfCarrierMissing } from "./startup-warning.ts";
import { repairActive } from "./auto-reconcile.ts";
import type { EngineBridge, SenpiNamespace } from "./engine-bridge.ts";
import { createTaskTruth, peerPolicy } from "./task-truth.ts";
import type { TaskTruth } from "./task-truth.ts";
import type { TargetRef } from "./contracts.ts";
import { connectManagedQuestionChild, createOwnedQuestions, createRemoteQuestionOwner, publishQuestionBroker } from "./peer-questions.ts";
import type { OwnedQuestions, QuestionOwner } from "./peer-questions.ts";
import type { CapturedRuntime } from "./runtime-capture.ts";
import type { RemoteRuntime } from "./peer-channel.ts";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const STATE = Symbol.for("omo.agent-coordination.capture.v1");
// Resolve while the real extension loader imports the entry, rather than in a
// later native factory callback. This keeps its SDK alias/namespace in force.
const namespace = await import("@code-yeongyu/senpi" as string) as SenpiNamespace & {
  VERSION: string; buildNoticeBox(spec: object, options: object, theme: unknown): unknown;
};
interface Context { sessionManager: { getSessionId(): string }; agentDir: string }
interface ExtensionAPI {
  on(name: string, handler: (event: unknown, context: Context) => unknown): void;
  registerTool(tool: {
    name: string; label: string; description: string; parameters: object;
    execute(id: string, args: Record<string, any>, signal: AbortSignal | undefined,
      update: unknown, context: Context): Promise<object>;
    renderResult?(result: any, options: { expanded: boolean }, theme: any): any;
  }): void;
}
export interface CaptureExtensionState {
  readonly entry_url: string;
  readonly generation: number;
  readonly capture: RuntimeCapture;
  readonly bridges: WeakMap<object, EngineBridge>;
  readonly inspections: WeakMap<object, TaskTruth>;
  readonly questions: WeakMap<object, OwnedQuestions>;
  readonly tools: readonly Parameters<ExtensionAPI["registerTool"]>[0][];
  readonly unsupported: Set<string>;
  rosterRevision(runtime: object): { revision: number; consumed: number } | undefined;
  connectRemote(context: Context, capture: CapturedRuntime, connection: RemoteRuntime):
    ReturnType<typeof createRemoteQuestionOwner>;
  dispose(): Promise<void>;
}

/** The loader imports this entry before bindExtensions; child packages load this same .js.
 * Senpi stays external, so the loader's actual SDK constructor is never bundled or copied.
 */
export default async function coordinationCapture(pi: ExtensionAPI) {
  const global = globalThis as unknown as Record<symbol, unknown>;
  const previous = global[STATE] as CaptureExtensionState | undefined;
  await previous?.dispose();
  const generation = (previous?.generation ?? 0) + 1;
  const capture = createRuntimeCapture({ loaderNamespace: namespace, generation,
    // connectEngineBridge validates the carrier and owning engine before forwarding
    // readiness. R13 sets module to the exact native SDK import before notifying it.
    nativeNamespace: () => (global[Symbol.for("omo.agent-coordination.native.v1")] as
      { module?: SenpiNamespace } | undefined)?.module,
  });
  const bridges = new WeakMap<object, EngineBridge>();
  const active = new Set<EngineBridge>();
  const inspections = new WeakMap<object, TaskTruth>();
  const activeInspections = new Set<TaskTruth>();
  const questions = new WeakMap<object, OwnedQuestions>();
  const activeQuestions = new Set<OwnedQuestions>();
  const activeOwners = new Set<QuestionOwner>();
  const remoteOwners = new Set<{ dispose(): void }>();
  const remoteReady = new WeakMap<object, Promise<void>>();
  const shutdown = new AbortController();
  const readyKey = Symbol.for("omo.agent-coordination.api-ready.v1");
  let publication: ReturnType<typeof publishQuestionBroker>;
  const publish = () => {
    if (!disposed && activeOwners.size && !publication) publication = publishQuestionBroker(
      activeOwners, globalThis, [...active].some(repairActive));
    return publication?.broker;
  };
  const unsupported = new Set<string>();
  const contexts = new WeakMap<object, Context>();
  const tools: Parameters<ExtensionAPI["registerTool"]>[0][] = [];
  const summary = (result: any): string => !result.ok
    ? `Unavailable: ${result.connection ?? result.reason}.`
    : result.value?.state ? `${result.value.state}${result.value.reason ? `: ${result.value.reason}` : ""}; context ${result.value.incorporation ?? "not incorporated"}.`
    : result.value?.coverage ? `Coverage ${result.value.coverage}: ${result.value.visited}/${result.value.owned} visited, ${result.value.unreadable} unreadable.`
    : `State ${result.value?.status ?? "unknown"}; record consistent: ${result.value?.record_consistent ?? "unknown"}.`;
  const registerTool = (tool: Parameters<ExtensionAPI["registerTool"]>[0]) => {
    tool.renderResult = (result, options, theme) => {
      const details = result.details ?? { ok: false, reason: "unsupported_runtime" };
      return namespace.buildNoticeBox({ title: tool.label, tone: details.ok ? "accent" : "warning",
        why: summary(details), extra: details.value?.reply ? [{ text: details.value.reply }] : [],
        expandedLine: JSON.stringify(details) }, options, theme);
    };
    tools.push(tool); pi.registerTool(tool);
  };
  const toolSurfaces = new Map<WeakRef<any>, { definitions: Set<object>; allowed: Set<string>; activated: Set<string> }>();
  const projections = new Map<WeakRef<any>, {
    before: any; wrapped: any; stop(): void; roster: { revision: number; consumed: number };
  }>();
  let disposed = false;
  let disposal: Promise<void> | undefined;
  // exposeTools runs on every agent event, and listOwned stats every owned task record
  // synchronously, so this path reuses one listing per bridge for a short window.
  const OWNED_LISTING_TTL_MS = 1000;
  const ownedListings = new WeakMap<object, { at: number; listed: ReturnType<EngineBridge["listOwned"]> }>();
  const listOwnedRecent = (bridge: EngineBridge) => {
    const now = Date.now(), cached = ownedListings.get(bridge);
    if (cached && now - cached.at < OWNED_LISTING_TTL_MS) return cached.listed;
    const listed = bridge.listOwned();
    ownedListings.set(bridge, { at: now, listed });
    return listed;
  };
  const exposeTools = (captured: Parameters<Parameters<typeof capture.onRuntime>[0]>[0]) => {
      const runtime = captured.runtime() as any;
      if (disposed || !runtime || typeof runtime.setActiveToolsByName !== "function") return false;
      if (bridges.has(runtime.sessionManager)) return true;
      for (const bridge of active) {
        const listed = listOwnedRecent(bridge);
        if (!listed.ok) continue;
        for (const snapshot of listed.value) {
          if (!peerPolicy(snapshot.role, snapshot.collaboration).ask_capability) continue;
          const bound = capture.bindOwned(bridge, snapshot.identity.target_ref, captured);
          if (!bound.ok) continue;
          if (!Array.isArray(runtime._customTools) || typeof runtime._refreshToolRegistry !== "function" ||
              runtime._allowedToolNames !== undefined && !(runtime._allowedToolNames instanceof Set)) {
            unsupported.add("collaboration_tool_registry_shape"); return false;
          }
          const missing = tools.filter((tool) => !runtime.getToolDefinition(tool.name));
          if (missing.length) {
            // The native readonly allowlist removed these definitions before
            // activation. Extend only this exact owned runtime, never its role
            // configuration, SDK prototype, or native task record.
            const definitions = new Set<object>(), allowed = new Set<string>();
            for (const tool of missing) {
              if (!runtime._customTools.some((entry: any) => entry.name === tool.name)) {
                runtime._customTools = [...runtime._customTools, tool]; definitions.add(tool);
              }
              if (runtime._allowedToolNames && !runtime._allowedToolNames.has(tool.name)) {
                runtime._allowedToolNames = new Set([...runtime._allowedToolNames, tool.name]); allowed.add(tool.name);
              }
            }
            toolSurfaces.set(new WeakRef(runtime), { definitions, allowed, activated: new Set() });
            runtime._refreshToolRegistry({ addedToolNames: missing.map((tool) => tool.name) });
          } else {
            const current: string[] = runtime.getActiveToolNames();
            const additions = tools.map((tool) => tool.name).filter((name) => !current.includes(name));
            if (additions.length) {
              runtime.setActiveToolsByName([...current, ...additions]);
              toolSurfaces.set(new WeakRef(runtime), {
                definitions: new Set(), allowed: new Set(), activated: new Set(additions),
              });
            }
          }
          return true;
        }
      }
      return false;
  };
  const stop = capture.onRuntime((captured, event) => {
    if (disposed) return;
    if (event === "changed") { exposeTools(captured); return; }
    if (event !== "bound") return;
    const runtime = captured.runtime() as any;
    if (runtime?.agent && typeof runtime.agent.prepareRequest === "function") {
      const roster = { revision: -1, pending: -1, consumed: -1, text: "" };
      const stopRoster = runtime.subscribe((event: any) => {
        if (event.type === "message_end" && event.message?.role === "assistant" &&
            !["error", "aborted"].includes(event.message.stopReason)) roster.consumed = roster.pending;
      });
      const before: (this: unknown, request: any, signal: AbortSignal) =>
        Promise<{ context?: any } | undefined> = runtime.agent.prepareRequest;
      const wrapped = async function (this: any, request: any, signal: AbortSignal) {
        await remoteReady.get(runtime.sessionManager);
        const previous = await before.call(this, request, signal);
        const exposed = exposeTools(captured);
        const context = runtime.extensionRunner.createContext();
        const owner = [...activeOwners].findLast((candidate) => candidate.bindContext(context).ok);
        if (!exposed && !owner) return previous;
        // The fork has already materialized this request's tool array. Project
        // only the newly authorized coordination definitions into that clone.
        const prepared = previous?.context ?? request.context;
        const current = prepared.tools ?? [];
        const names = new Set(current.map((tool: any) => tool.name));
        const additions = runtime.agent.state.tools.filter((tool: any) =>
          tools.some((definition) => definition.name === tool.name) && !names.has(tool.name));
        const peers = owner?.peers();
        if (peers?.ok && peers.value.revision !== roster.revision) {
          roster.revision = peers.value.revision;
          roster.text = `\n\n<omo-agent-coordination-roster-v1>\n${JSON.stringify(peers.value)}\n</omo-agent-coordination-roster-v1>`;
        }
        roster.pending = roster.revision;
        // A cached request-local section has no transcript/history entry or
        // session/global prompt setter. An unchanged revision replaces nothing
        // and cannot accumulate repeated announcements or start another turn.
        return { ...previous, context: { ...prepared, tools: additions.length ? [...current, ...additions] : current,
          systemPrompt: `${prepared.systemPrompt ?? ""}${roster.text}` } };
      };
      runtime.agent.prepareRequest = wrapped;
      projections.set(new WeakRef(runtime), { before, wrapped, stop: stopRoster, roster });
    }
    const context = runtime && contexts.get(runtime.sessionManager);
    if (context && process.env.OMO_SENPI_TASK_RPC_CHILD === "1" && !remoteReady.has(context.sessionManager)) {
      const pending = connectManagedQuestionChild({ context, capture: captured,
        captureStore: capture, signal: shutdown.signal }).then((result) => {
        if (result.ok) {
          if (disposed) { result.value.dispose(); return; }
          activeOwners.add(result.value.owner); remoteOwners.add(result.value); publish();
        } else unsupported.add(`managed_child_bootstrap:${result.reason}`);
      });
      remoteReady.set(context.sessionManager, pending);
    }
    if (!context || bridges.has(context.sessionManager)) return;
    const connection = connectEngineBridge({
      context, generation, loaderNamespace: namespace,
      onModuleReady: (actual) => {
        const ready = capture.moduleReady(actual);
        if (!ready.ok) throw new Error(`CAPTURE_UNSUPPORTED: ${ready.connection}`);
      },
    });
    if (connection.ok) {
      bridges.set(context.sessionManager, connection.value);
      active.add(connection.value);
      let collaboration: OwnedQuestions | undefined;
      const truth = createTaskTruth({ bridge: connection.value, capture, owner: captured, generation,
        asks: (ref) => collaboration?.asks(ref) ?? "allowed" });
      inspections.set(context.sessionManager, truth);
      activeInspections.add(truth);
      collaboration = createOwnedQuestions({ context, owner: captured, capture,
        bridge: connection.value, truth });
      questions.set(context.sessionManager, collaboration);
      activeQuestions.add(collaboration);
      activeOwners.add(collaboration);
    } else unsupported.add(connection.connection ?? connection.reason);
  });
  const state: CaptureExtensionState = Object.freeze({
    entry_url: import.meta.url, generation, capture, bridges, inspections, questions, tools, unsupported,
    rosterRevision(runtime: object) {
      for (const [ref, projection] of projections) if (ref.deref() === runtime) {
        return { revision: projection.roster.revision, consumed: projection.roster.consumed };
      }
      return undefined;
    },
    async connectRemote(context: Context, captured: CapturedRuntime, connection: RemoteRuntime) {
      if (disposed || !capture.status.ok || !capture.list().includes(captured)) {
        return { ok: false as const, reason: "scope_denied" as const };
      }
      const result = await createRemoteQuestionOwner({ context, capture: captured, captureStore: capture, connection });
      if (result.ok) {
        activeOwners.add(result.value.owner); remoteOwners.add(result.value); publish();
      }
      return result;
    },
    dispose() {
      if (disposed) return disposal ?? Promise.resolve();
      disposed = true;
      shutdown.abort();
      stop();
      publication?.dispose();
      if (global[readyKey] === publish) delete global[readyKey];
      for (const [ref, projection] of projections) {
        projection.stop();
        const runtime = ref.deref();
        if (runtime?.agent.prepareRequest === projection.wrapped) runtime.agent.prepareRequest = projection.before;
      }
      projections.clear();
      for (const [ref, surface] of toolSurfaces) {
        const runtime = ref.deref();
        if (!runtime || runtime._disposed) continue;
        runtime._customTools = runtime._customTools.filter((definition: object) => !surface.definitions.has(definition));
        if (runtime._allowedToolNames) runtime._allowedToolNames = new Set([...runtime._allowedToolNames]
          .filter((name: string) => !surface.allowed.has(name)));
        runtime._refreshToolRegistry();
        if (surface.activated.size) runtime.setActiveToolsByName(runtime.getActiveToolNames()
          .filter((name: string) => !surface.activated.has(name)));
      }
      toolSurfaces.clear();
      const pending = [...activeQuestions].map((collaboration) => collaboration.dispose());
      for (const owner of remoteOwners) owner.dispose();
      activeQuestions.clear(); activeOwners.clear(); remoteOwners.clear();
      for (const truth of activeInspections) truth.dispose();
      activeInspections.clear();
      for (const bridge of active) bridge.dispose();
      active.clear();
      capture.dispose();
      return disposal = Promise.all(pending).then(() => undefined);
    },
  });
  global[STATE] = state;
  if (capture.status.ok) global[readyKey] = publish;
  if (!capture.status.ok) unsupported.add(capture.status.connection ?? capture.status.reason);
  if (capture.status.ok) registerTool({
    name: "coordination_inspect", label: "Inspect owned coordination",
    description: "Read the current owned task tree or one issued target_ref. Shows runtime activity, work outcome, publication, notification, residency and coverage separately. No model call or task state changes.",
    parameters: { type: "object", properties: { target_ref: { type: "string" } }, additionalProperties: false },
    async execute(_id, args, _signal, _update, context) {
      const truth = inspections.get(context.sessionManager);
      const owner = [...activeOwners].findLast((candidate) => candidate.bindContext(context).ok);
      const result = args.target_ref !== undefined && typeof args.target_ref !== "string"
        ? { ok: false, reason: "invalid_target" } : owner
          ? await owner.inspect(args.target_ref as TargetRef | undefined) : truth
          ? await truth.inspect(args.target_ref as TargetRef | undefined)
          : { ok: false, reason: disposed ? "stale_generation" : "unsupported_runtime",
            ...(!disposed && capture.status.ok ? { connection: "capture_missing" } : {}) };
      return { content: [{ type: "text", text: `${summary(result)}\n${JSON.stringify(result)}` }], details: result, isError: !result.ok };
    },
  });
  for (const operation of ["ask", "notify"] as const) if (capture.status.ok) registerTool({
    name: `coordination_${operation}`, label: operation === "ask" ? "Ask an owned peer" : "Notify an owned peer",
    description: operation === "ask"
      ? "Ask a same-tree peer directly about an unexpected state, shared-resource conflict, or judgment tools cannot answer. Uses the target's live context for one tool-free extra model request without steering its work. Quick is the default cheap answer; deep blocks the asker until the reply arrives and costs the target model's full price. Use inspect for status, native task_send for instructions, and notify for one-way information. Keep it short, include artifact references, and do not retry a missing answer in a loop. A peer reply is not user approval."
      : "Send one explicit same-tree peer a short note or artifact reference. It costs zero immediate model requests and is queued for that peer's next natural turn, not read immediately. It neither starts nor interrupts a turn. Use inspect for status, ask for a judgment, and native task_send for instructions.",
    parameters: { type: "object", properties: {
      target_ref: { type: "string" }, request_id: { type: "string" },
      ...(operation === "ask" ? { question: { type: "string" },
        depth: { type: "string", enum: ["quick", "deep"] }, timeout_ms: { type: "integer", minimum: 1, maximum: 60000 } }
        : { message: { type: "string" } }),
    }, required: ["target_ref", operation === "ask" ? "question" : "message"], additionalProperties: false },
    async execute(id, args, signal, _update, context) {
      const owner = [...activeOwners].findLast((candidate) => candidate.bindContext(context).ok);
      publish();
      const result = owner ? await owner.request(context, operation, { ...args, request_id: args.request_id ?? id }, signal)
        : { ok: false, reason: disposed ? "stale_generation" : activeOwners.size ? "scope_denied" : "unsupported_runtime",
          ...(!disposed && !activeOwners.size && capture.status.ok ? { connection: "capture_missing" } : {}) };
      return { content: [{ type: "text", text: `${summary(result)}\n${JSON.stringify(result)}` }], details: result, isError: !result.ok };
    },
  });
  pi.on("session_start", (_event, context) => {
    contexts.set(context.sessionManager, context);
    warnIfCarrierMissing(context as never, namespace, generation);
  });
  // This official event follows all session_start handlers, including the
  // native owner's binding. Adopting earlier can capture before that link exists.
  pi.on("resources_discover", (_event, context) => {
    capture.adopt(context.sessionManager);
  });
  pi.on("session_shutdown", () => state.dispose());
  pi.on("session_extensions_removed", (event) => {
    const removed = (event as { removed: { path: string; resolvedPath: string }[] }).removed;
    if (removed.some((entry) => [entry.path, entry.resolvedPath]
      .some((file) => pathToFileURL(resolve(file)).href === state.entry_url))) return state.dispose();
  });
}
