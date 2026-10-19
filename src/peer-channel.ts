import { createHash, randomBytes, randomUUID } from "node:crypto";
import { createConnection, createServer } from "node:net";
import type { Socket } from "node:net";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type {
  Asks, CoordinationBroker, CoordinationClient, Feature, Result, Roster, TargetBinding,
  TargetRef, TerminalReason,
} from "./contracts.ts";
import { LIMITS } from "./contracts.ts";
import type { BridgeResult, EngineBridge, OwnedSnapshot } from "./engine-bridge.ts";
import type { CapturedRuntime, RuntimeCapture } from "./runtime-capture.ts";

type Rejected = { readonly ok: false; readonly reason: TerminalReason; readonly reachability?: "unreachable" };
type Reply = { readonly ok: true; readonly value: unknown } | Rejected;
type Context = { sessionManager: object; agentDir: string };
export interface PeerRequest {
  readonly target_ref: TargetRef;
  readonly request_id: string;
  readonly operation: string;
  readonly payload: unknown;
  readonly signal?: AbortSignal;
}
export interface PeerCall extends PeerRequest {
  readonly caller: TargetBinding;
  readonly target: TargetBinding;
}
export type PeerHandler = (call: PeerCall) => Reply | Promise<Reply>;
export type PeerHandlers = Readonly<Record<string, PeerHandler>>;
export interface RemoteInvitation {
  readonly address: string;
  readonly token: string;
  readonly generation: number;
  readonly owner_session_id: string;
  readonly owner_root_id: string;
}
interface Hello {
  token: string; generation: number; pid: number; session_id: string;
  runtime_instance_id: string; branch: string;
}
const normalize = (id: string) => id.replace(/^senpi:/, "");
const unavailable = (): Rejected => ({ ok: false, reason: "host_failure", reachability: "unreachable" });
const refuse = (reason: TerminalReason): Rejected => ({ ok: false, reason });
const digest = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};
function requestBytes(value: unknown) { return Buffer.byteLength(JSON.stringify(value)); }
function bounded<T>(pending: Promise<T>, label: string, ms = 30_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([pending, new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`CHANNEL_DEADLINE: ${label}`)), ms);
  })]).finally(() => clearTimeout(timer!));
}

/** One duplex link, no reconnect/retry. Only requests have a total size limit.
 * Response chunks have sequence, total length, and a digest; UTF-8 is decoded once.
 */
class Wire {
  private buffered = Buffer.alloc(0);
  private pending = new Map<string, {
    resolve(value: Reply): void; stop(): void; chunks: Buffer[];
    length?: number; hash?: string; count?: number; received: number;
  }>();
  private incoming = new Map<string, AbortController>();
  private closed = false;
  readonly socket: Socket;
  private receive: (value: any, signal: AbortSignal) => Promise<Reply>;
  readonly finished = deferred<void>();
  frames = 0;
  constructor(socket: Socket, receive: (value: any, signal: AbortSignal) => Promise<Reply>) {
    this.socket = socket;
    this.receive = receive;
    socket.on("error", () => this.close());
    socket.on("close", () => this.close());
    socket.on("data", (bytes: Buffer) => {
      if (this.closed) return;
      this.buffered = Buffer.concat([this.buffered, bytes]);
      for (;;) {
        const end = this.buffered.indexOf(10);
        if (end < 0) {
          if (this.buffered.length > LIMITS.request_frame_utf8_bytes) this.close();
          return;
        }
        if (end > LIMITS.request_frame_utf8_bytes) { this.close(); return; }
        const line = this.buffered.subarray(0, end);
        this.buffered = this.buffered.subarray(end + 1);
        try { this.frame(JSON.parse(line.toString("utf8"))); }
        catch { this.close(); return; }
      }
    });
  }
  get isClosed() { return this.closed; }
  get pendingCount() { return this.pending.size + this.incoming.size; }
  private send(frame: object) {
    if (!this.closed) this.socket.write(JSON.stringify(frame) + "\n");
  }
  private frame(frame: any) {
    if (!frame || typeof frame.id !== "string") throw new Error("invalid frame");
    if (frame.type === "request") {
      if (this.incoming.has(frame.id)) throw new Error("duplicate wire id");
      const controller = new AbortController();
      this.incoming.set(frame.id, controller);
      void this.receive(frame.value, controller.signal).then((result) => {
        if (this.closed || controller.signal.aborted) return;
        const bytes = Buffer.from(JSON.stringify(result), "utf8");
        const size = 32 * 1024;
        this.send({ type: "begin", id: frame.id, length: bytes.length,
          count: Math.ceil(bytes.length / size), hash: digest(bytes) });
        for (let offset = 0, seq = 0; offset < bytes.length; offset += size, seq++) {
          this.send({ type: "chunk", id: frame.id, seq, data: bytes.subarray(offset, offset + size).toString("base64") });
        }
        this.send({ type: "end", id: frame.id });
      }, () => { this.close(); }).finally(() => this.incoming.delete(frame.id));
      return;
    }
    if (frame.type === "cancel") {
      this.incoming.get(frame.id)?.abort();
      return;
    }
    const call = this.pending.get(frame.id);
    // A cancelled request's late response is discarded, never adopted.
    if (!call) return;
    if (frame.type === "begin") {
      if (call.length !== undefined || !Number.isSafeInteger(frame.length) || frame.length < 1 ||
          !Number.isSafeInteger(frame.count) || frame.count !== Math.ceil(frame.length / (32 * 1024)) ||
          typeof frame.hash !== "string" || !/^[a-f0-9]{64}$/.test(frame.hash)) throw new Error("invalid header");
      call.length = frame.length; call.hash = frame.hash; call.count = frame.count;
    } else if (frame.type === "chunk") {
      if (call.length === undefined || frame.seq !== call.chunks.length ||
          frame.seq >= call.count! || typeof frame.data !== "string" ||
          !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(frame.data)) {
        throw new Error("invalid chunk");
      }
      const bytes = Buffer.from(frame.data, "base64");
      if (!bytes.length || bytes.length > 32 * 1024 ||
          (frame.seq < call.count! - 1 && bytes.length !== 32 * 1024)) throw new Error("invalid chunk length");
      call.chunks.push(bytes); call.received += bytes.length; this.frames++;
      if (call.received > call.length) throw new Error("response overflow");
    } else if (frame.type === "end") {
      const bytes = Buffer.concat(call.chunks);
      if (call.length !== bytes.length || call.count !== call.chunks.length ||
          digest(bytes) !== call.hash) throw new Error("response digest mismatch");
      const value = JSON.parse(bytes.toString("utf8"));
      if (!value || typeof value.ok !== "boolean") throw new Error("invalid result");
      this.pending.delete(frame.id); call.stop(); call.resolve(value);
    } else throw new Error("unknown frame");
  }
  call(value: unknown, signal?: AbortSignal): Promise<Reply> {
    if (this.closed) return Promise.resolve(unavailable());
    const id = randomUUID();
    const frame = { type: "request", id, value };
    if (requestBytes(frame) > LIMITS.request_frame_utf8_bytes) return Promise.resolve(refuse("invalid_argument"));
    if (signal?.aborted) return Promise.resolve(refuse("asker_aborted"));
    return new Promise((resolve) => {
      const abort = () => {
        const call = this.pending.get(id);
        if (!call) return;
        this.pending.delete(id); call.stop();
        this.send({ type: "cancel", id }); resolve(refuse("asker_aborted"));
      };
      this.pending.set(id, { resolve, stop: () => signal?.removeEventListener("abort", abort),
        chunks: [], received: 0 });
      signal?.addEventListener("abort", abort, { once: true });
      this.send(frame);
    });
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.socket.destroy();
    for (const call of this.pending.values()) { call.stop(); call.resolve(unavailable()); }
    this.pending.clear();
    for (const controller of this.incoming.values()) controller.abort();
    this.incoming.clear(); this.buffered = Buffer.alloc(0);
    this.finished.resolve();
  }
}

/** All authority comes from the existing native owner bridge and observed local capture.
 * An invitation is a launch capability, not a target address; it becomes usable only
 * after binding to the original native RPC handle, current epoch, PID and live branch.
 */
export async function createPeerChannel(options: {
  context: Context; root: CapturedRuntime; capture: RuntimeCapture; bridge: EngineBridge;
  handlers?: PeerHandlers;
  authorize?: (caller: TargetBinding, request: PeerRequest) => Result<unknown>;
  ownerOperations?: readonly string[];
  onRemoteEvent?: (binding: TargetBinding, event: unknown) => void;
  roster?: { peers(): BridgeResult<Roster>; onPeersChanged(listener: (roster: Roster) => void): () => void };
}) {
  const rootRuntime = options.root.runtime();
  const rootSnapshot = options.root.read();
  const carrier = (globalThis as any)[Symbol.for("omo.agent-coordination.native.v1")];
  const engineRef = rootRuntime && carrier?.contexts.get(rootRuntime.sessionManager);
  const engine = engineRef?.deref();
  const owned = options.bridge.listOwned();
  if (process.platform !== "win32" || !rootRuntime || !rootSnapshot.ok || !owned.ok ||
      options.context.sessionManager !== rootRuntime.sessionManager || !engine ||
      normalize(engine.runtime.sessionId() ?? "") !== normalize(rootRuntime.sessionId)) {
    return { ok: false as const, reason: "scope_denied" as const };
  }
  const generation = options.capture.generation;
  const issuedRoster = options.roster?.peers();
  if (issuedRoster && !issuedRoster.ok) return { ok: false as const, reason: "scope_denied" as const };
  const workerRefs = new Set(owned.value.map((item) => item.identity.target_ref));
  const issuedRoot = issuedRoster?.ok ? issuedRoster.value.peers.filter((peer) => !workerRefs.has(peer.target_ref)) : [];
  if (options.roster && issuedRoot.length !== 1) return { ok: false as const, reason: "scope_denied" as const };
  const rootBinding: TargetBinding = Object.freeze({
    kind: "root", target_ref: issuedRoot[0]?.target_ref ?? `coordination:${randomUUID()}` as TargetRef,
    owner_session_id: rootRuntime.sessionId, owner_root_id: engine.ancestry?.rootSessionId ?? rootRuntime.sessionId,
    canonical_session_id: rootRuntime.sessionId, session_id: normalize(rootRuntime.sessionId),
    runtime_instance_id: options.root.runtime_instance_id, generation,
    branch: rootSnapshot.value.branch ?? `session:${rootRuntime.sessionId}`,
  });
  const stateRoot = path.join(path.resolve(options.context.agentDir), "agent-coordination");
  const address = "\\\\.\\pipe\\omo-coordination-" + digest(JSON.stringify([stateRoot, rootBinding])).slice(0, 48);
  const sockets = new Set<Wire>();
  const remote = new Map<TargetRef, { binding: TargetBinding; wire: Wire; handle: WeakRef<any> }>();
  const tickets = new Map<string, {
    invitation: RemoteInvitation; hello: ReturnType<typeof deferred<{ value: Hello; wire: Wire }>>;
    approved: ReturnType<typeof deferred<Reply>>; used: boolean; connected: boolean; target?: TargetRef;
  }>();
  const ledger = new Map<string, { hash: string; result: Promise<Reply> }>();
  const listeners = new Set<(roster: Roster) => void>();
  const shutdown = new AbortController();
  let disposed = false, pending = 0, revision = 0, signature = "";
  let roster: Roster = Object.freeze({ revision: 0, peers: [] });
  const current = (): Rejected | undefined => {
    if (disposed || options.capture.generation !== generation) return refuse("stale_generation");
    const snapshot = options.root.read();
    if (!snapshot.ok || options.root.runtime() !== rootRuntime || rootRuntime._disposed) return refuse("stale_generation");
    const check = options.bridge.listOwned();
    if (!check.ok) return refuse(check.reason === "stale_generation" ? "stale_generation" : "scope_denied");
    return undefined;
  };
  const worker = (target: TargetRef): Result<OwnedSnapshot> => {
    const result = options.bridge.inspectOwned(target);
    return result.ok ? result : { ok: false, reason:
      result.reason === "runtime_mismatch" ? "invalid_target" :
      result.reason === "connection_failed" || result.reason === "repair_not_enabled" ? "unsupported_runtime" : result.reason };
  };
  const target = (ref: TargetRef): Result<{ binding: TargetBinding; capture?: CapturedRuntime; wire?: Wire }> => {
    const stale = current();
    if (stale) return stale;
    if (ref === rootBinding.target_ref) return { ok: true, value: { binding: rootBinding, capture: options.root } };
    const snapshot = worker(ref);
    if (!snapshot.ok) return snapshot;
    if (!snapshot.value.collaboration.allowed) return { ok: false, reason: "scope_denied" };
    if (["completed", "error", "cancelled", "interrupted", "lost"].includes(snapshot.value.status)) {
      return { ok: false, reason: "target_settled" };
    }
    const item = remote.get(ref);
    if (item) {
      if (item.wire.isClosed) return unavailable();
      const native = item.handle.deref();
      if (snapshot.value.identity.run_epoch !== (item.binding.kind === "worker" ? item.binding.run_epoch : null)) {
        return { ok: false, reason: "stale_epoch" };
      }
      if (!native || engineRef.deref()?.manager.getResidentHandle(snapshot.value.identity.task_id) !== native ||
          native.hasExited()) return { ok: false, reason: "target_not_live" };
      return { ok: true, value: { binding: item.binding, wire: item.wire } };
    }
    for (const capture of options.capture.list()) {
      const runtime = capture.runtime();
      if (!runtime || normalize(runtime.sessionId) !== snapshot.value.identity.session_id) continue;
      const bound = options.capture.bindOwned(options.bridge, ref, capture);
      if (!bound.ok || !bound.value.session_id || !bound.value.canonical_session_id) continue;
      const binding: TargetBinding = Object.freeze({ ...bound.value, kind: "worker",
        canonical_session_id: bound.value.canonical_session_id, session_id: bound.value.session_id,
        runtime_instance_id: capture.runtime_instance_id });
      return { ok: true, value: { binding, capture } };
    }
    return { ok: false, reason: "context_unavailable" };
  };
  const bindContext = (ctx: unknown): Result<TargetBinding> => {
    const stale = current();
    if (stale) return stale;
    if (!ctx || typeof ctx !== "object") return { ok: false, reason: "scope_denied" };
    const manager = (ctx as { sessionManager?: object }).sessionManager;
    if (manager === rootRuntime.sessionManager) return { ok: true, value: rootBinding };
    const owned = options.bridge.listOwned();
    if (!owned.ok) return { ok: false, reason: "scope_denied" };
    for (const snapshot of owned.value) {
      const valid = target(snapshot.identity.target_ref);
      if (valid.ok && valid.value.capture?.runtime()?.sessionManager === manager) {
        return { ok: true, value: valid.value.binding };
      }
    }
    return { ok: false, reason: "scope_denied" };
  };
  const refresh = (): Result<Roster> => {
    const stale = current();
    if (stale) return stale;
    if (options.roster) {
      const shared = options.roster.peers();
      if (!shared.ok) return refuse(shared.reason === "stale_generation" ? "stale_generation" : "unsupported_runtime");
      const next = JSON.stringify(shared.value.peers);
      if (next !== signature) {
        signature = next; roster = shared.value; revision = roster.revision;
        for (const listener of [...listeners]) listener(roster);
      }
      return { ok: true, value: roster };
    }
    const owned = options.bridge.listOwned();
    if (!owned.ok) return { ok: false, reason: "scope_denied" };
    const peers = [{ target_ref: rootBinding.target_ref, name: "root/main", role: "main",
      ask_capability: true, notify_capability: true, asks: "allowed" as const },
    ...owned.value.map((snapshot) => ({
      target_ref: snapshot.identity.target_ref, name: snapshot.identity.task_id, role: snapshot.role,
      ask_capability: snapshot.collaboration.allowed, notify_capability: snapshot.collaboration.allowed,
      asks: snapshot.collaboration.allowed ? "allowed" as const : "denied" as const,
      ...(snapshot.collaboration.denial_reason ? { denial_reason: snapshot.collaboration.denial_reason } : {}),
    }))];
    const next = JSON.stringify(peers);
    if (next !== signature) {
      signature = next;
      roster = Object.freeze({ revision: ++revision, peers: Object.freeze(peers.map((peer) => Object.freeze(peer))) });
      for (const listener of [...listeners]) listener(roster);
    }
    return { ok: true, value: roster };
  };
  const dispatch = async (caller: TargetBinding, request: PeerRequest): Promise<Reply> => {
    const stale = current();
    if (stale) return stale;
    if (!request || typeof request.request_id !== "string" || !request.request_id ||
        typeof request.operation !== "string" || !request.operation ||
        typeof request.target_ref !== "string") return refuse("invalid_argument");
    const { signal, ...content } = request;
    if (requestBytes(content) > LIMITS.request_frame_utf8_bytes) return refuse("invalid_argument");
    const asker = target(caller.target_ref);
    if (!asker.ok) return asker;
    if (asker.value.binding.runtime_instance_id !== caller.runtime_instance_id) return refuse("stale_epoch");
    const authorized = options.authorize?.(caller, request);
    if (authorized && !authorized.ok) return authorized;
    const key = JSON.stringify([caller.target_ref, request.request_id]);
    const hash = digest(JSON.stringify([request.operation, request.target_ref, request.payload],
      (_key, value) => value && typeof value === "object" && !Array.isArray(value)
        ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b))) : value));
    const existing = ledger.get(key);
    if (existing) {
      if (request.target_ref !== rootBinding.target_ref) {
        const permission = worker(request.target_ref);
        if (!permission.ok) return permission;
        if (!permission.value.collaboration.allowed) return refuse("scope_denied");
      }
      if (existing.hash !== hash) return refuse("conflict");
      const result = await existing.result;
      const authorized = options.authorize?.(caller, request);
      return authorized && !authorized.ok ? authorized : result;
    }
    if (request.operation === "setAskPolicy" && caller.kind !== "root") return refuse("policy_not_owner");
    if (request.operation === "setAskPolicy" && request.target_ref === rootBinding.target_ref) return refuse("invalid_target");
    const recipient = target(request.target_ref);
    if (!recipient.ok) return recipient;
    const gate = deferred<Reply>();
    ledger.set(key, { hash, result: gate.promise });
    pending++;
    const combined = signal ? AbortSignal.any([signal, shutdown.signal]) : shutdown.signal;
    void new Promise<Reply>((resolve) => {
      let finished = false;
      const commit = (result: Reply) => {
        if (finished) return;
        finished = true; combined.removeEventListener("abort", abort);
        const stale = current();
        const freshCaller = target(caller.target_ref), freshTarget = target(request.target_ref);
        resolve(stale ?? (!freshCaller.ok ? freshCaller : !freshTarget.ok ? freshTarget : result));
      };
      const abort = () => commit(refuse("asker_aborted"));
      combined.addEventListener("abort", abort, { once: true });
      if (combined.aborted) { abort(); return; }
      const call: PeerCall = { ...request, caller, target: recipient.value.binding, signal: combined };
      const handler = options.handlers?.[request.operation];
      Promise.resolve().then(() => handler && options.ownerOperations?.includes(request.operation)
        ? handler(call) : recipient.value.wire
        ? recipient.value.wire.call({ kind: "invoke", call: { ...call, signal: undefined } }, combined)
        : handler ? handler(call) : refuse("coordination_unavailable"))
        .then(commit, () => commit(refuse("host_failure")));
    }).then(gate.resolve).finally(() => pending--);
    return gate.promise;
  };
  const server = createServer((socket) => {
    let ticket: typeof tickets extends Map<string, infer T> ? T : never;
    const wire = new Wire(socket, async (message, signal) => {
      if (message?.kind === "hello") {
        const found = tickets.get(message.hello?.token);
        if (!found || found.connected || found.used || message.hello?.generation !== generation) return refuse("scope_denied");
        found.connected = true; ticket = found;
        found.hello.resolve({ value: message.hello, wire });
        return found.approved.promise;
      }
      if (!ticket?.used || !ticket.target || !["route", "event"].includes(message?.kind)) return refuse("scope_denied");
      const caller = target(ticket.target);
      if (!caller.ok) return caller;
      if (message.kind === "event") {
        options.onRemoteEvent?.(caller.value.binding, message.value);
        return { ok: true, value: undefined };
      }
      return dispatch(caller.value.binding, { ...message.request, signal });
    });
    sockets.add(wire);
    void wire.finished.promise.then(() => { sockets.delete(wire); });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(address, () => { server.off("error", reject); resolve(); });
  });
  let closing: Promise<void> | undefined;
  const stopCapture = options.capture.onRuntime((capture, event) => {
    if (capture === options.root && event === "closed") void dispose();
  });
  const stopRoster = options.roster?.onPeersChanged(() => { refresh(); });
  function dispose(): Promise<void> {
    if (closing) return closing;
    disposed = true; shutdown.abort(); stopCapture(); stopRoster?.();
    for (const ticket of tickets.values()) ticket.approved.resolve(refuse("stale_generation"));
    for (const wire of sockets) wire.close();
    sockets.clear(); remote.clear(); tickets.clear(); listeners.clear(); ledger.clear();
    closing = new Promise<void>((resolve) => server.close(() => resolve()));
    return closing;
  }
  refresh();
  const channel = {
    root_ref: rootBinding.target_ref,
    bindContext,
    resolveTarget(ref: TargetRef) {
      const actual = target(ref);
      return actual.ok ? { ok: true as const, value: { binding: actual.value.binding,
        capture: actual.value.capture, remote: !!actual.value.wire } } : actual;
    },
    async invokeTarget(ref: TargetRef, operation: string, payload: unknown, signal?: AbortSignal): Promise<Reply> {
      const actual = target(ref);
      if (!actual.ok) return actual;
      if (!actual.value.wire || !operation.startsWith("coordination.question.")) return refuse("scope_denied");
      const combined = signal ? AbortSignal.any([signal, shutdown.signal]) : shutdown.signal;
      const result = await actual.value.wire.call({ kind: "invoke", call: {
        caller: rootBinding, target: actual.value.binding, target_ref: ref, request_id: randomUUID(), operation, payload,
      } }, combined);
      const fresh = target(ref), stale = current();
      return stale ?? (!fresh.ok ? fresh : fresh.value.binding.runtime_instance_id !== actual.value.binding.runtime_instance_id
        ? refuse("stale_epoch") : result);
    },
    refresh,
    peers: refresh,
    request(ctx: unknown, request: PeerRequest): Promise<Reply> {
      const caller = bindContext(ctx);
      return caller.ok ? dispatch(caller.value, request) : Promise.resolve(caller);
    },
    setAskPolicy(ctx: unknown, input: any): Result<{ asks: Asks }> {
      const caller = bindContext(ctx);
      if (!caller.ok) return caller;
      if (caller.value.kind !== "root") return refuse("policy_not_owner");
      if (input?.target_ref === rootBinding.target_ref) return refuse("invalid_target");
      const recipient = target(input?.target_ref);
      if (!recipient.ok) return recipient;
      const handler = options.handlers?.setAskPolicy;
      if (!handler) return refuse("coordination_unavailable");
      const result = handler({ target_ref: input.target_ref, request_id: randomUUID(),
        operation: "setAskPolicy", payload: input, caller: caller.value, target: recipient.value.binding });
      if (result instanceof Promise) return refuse("unsupported_runtime");
      if (!result.ok) return result;
      const value = result.value as { asks?: unknown } | null;
      return value?.asks === "allowed" || value?.asks === "denied"
        ? { ok: true, value: { asks: value.asks } } : refuse("unsupported_runtime");
    },
    onPeersChanged(listener: (roster: Roster) => void) {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    inviteRemote(): RemoteInvitation {
      if (current()) throw new Error("STALE_CHANNEL");
      const invitation = Object.freeze({ address, token: randomBytes(32).toString("hex"), generation,
        owner_session_id: rootBinding.owner_session_id, owner_root_id: rootBinding.owner_root_id });
      tickets.set(invitation.token, { invitation, hello: deferred(), approved: deferred(),
        used: false, connected: false });
      return invitation;
    },
    async remoteReady(invitation: RemoteInvitation): Promise<Pick<Hello, "pid" | "session_id" | "branch">> {
      const ticket = tickets.get(invitation.token);
      if (!ticket || ticket.invitation !== invitation) throw new Error("INVALID_REMOTE_INVITATION");
      const hello = await bounded(ticket.hello.promise, "native child hello");
      return { pid: hello.value.pid, session_id: hello.value.session_id, branch: hello.value.branch };
    },
    async bindRemote(invitation: RemoteInvitation, ref: TargetRef): Promise<Reply> {
      const ticket = tickets.get(invitation.token);
      if (!ticket || ticket.invitation !== invitation || ticket.used) return refuse("scope_denied");
      const snapshot = worker(ref);
      if (!snapshot.ok || !snapshot.value.collaboration.allowed) {
        const result = snapshot.ok ? refuse("scope_denied") : snapshot;
        ticket.approved.resolve(result);
        return result;
      }
      const hello = await bounded(ticket.hello.promise, "native child hello");
      const native = engineRef.deref()?.manager.getResidentHandle(snapshot.value.identity.task_id);
      if (native?.kind !== "rpc" || native.hasExited()) return refuse("target_not_live");
      const identityPort = native[Symbol.for("omo.agent-coordination.rpc-identity.v1")];
      if (typeof identityPort !== "function") {
        const result = refuse("unsupported_runtime"); ticket.approved.resolve(result); return result;
      }
      const identity = await bounded<{ session_id: string }>(identityPort(), "native RPC session identity");
      if (typeof identity.session_id !== "string" || typeof hello.value.session_id !== "string" ||
          normalize(identity.session_id) !== normalize(hello.value.session_id)) {
        const result = refuse("scope_denied"); ticket.approved.resolve(result); return result;
      }
      const proof = await bounded<{ entries: { id: string }[] }>(native.getEntries(), "native RPC branch");
      let processMatches = hello.value.pid === native.pid;
      if (!processMatches && Number.isSafeInteger(hello.value.pid) && hello.value.pid > 0 &&
          Number.isSafeInteger(native.pid) && native.pid > 0) {
        // The Windows package executable is a launcher, not necessarily the
        // runtime itself. A fresh OS ancestry/creation-order proof supplements
        // the original live handle and its authenticated native RPC branch.
        try {
          const { stdout } = await promisify(execFile)(
            path.join(process.env.SystemRoot ?? "C:/Windows", "System32/WindowsPowerShell/v1.0/powershell.exe"),
            ["-NoProfile", "-NonInteractive", "-Command",
              `$rows=@();$seen=@{};$id=${hello.value.pid};while($id -gt 0 -and !$seen.ContainsKey($id)){$seen[$id]=$true;$p=Get-CimInstance Win32_Process -Filter ("ProcessId="+$id);if(!$p){break};$rows+=[pscustomobject]@{pid=$p.ProcessId;parent=$p.ParentProcessId;created=$p.CreationDate.ToUniversalTime().ToString('o')};if($id -eq ${native.pid} -or $id -eq ${process.pid}){break};$id=[int]$p.ParentProcessId};ConvertTo-Json -InputObject @($rows) -Compress`],
            { timeout: 5_000, maxBuffer: 1024 * 1024, windowsHide: true });
          const rows = JSON.parse(stdout) as { pid: number; parent: number; created: string }[];
          const processes = new Map(rows.map(row => [row.pid, row]));
          const visited = new Set<number>();
          let child = processes.get(hello.value.pid);
          while (child && !visited.has(child.pid)) {
            visited.add(child.pid);
            const parent = processes.get(child.parent);
            if (!parent || !Number.isFinite(Date.parse(child.created)) ||
                !Number.isFinite(Date.parse(parent.created)) || Date.parse(parent.created) > Date.parse(child.created)) break;
            if (parent.pid === native.pid) { processMatches = true; break; }
            child = parent;
          }
        } catch {
          const result = refuse("host_failure"); ticket.approved.resolve(result); return result;
        }
      }
      const fresh = worker(ref);
      if (!fresh.ok) { ticket.approved.resolve(fresh); return fresh; }
      if (!processMatches || native.hasExited() || !hello.value.branch ||
          !proof.entries.some((entry) => entry.id === hello.value.branch) ||
          !hello.value.session_id || !hello.value.runtime_instance_id ||
          engineRef.deref()?.manager.getResidentHandle(snapshot.value.identity.task_id) !== native) {
        const result = refuse("scope_denied"); ticket.approved.resolve(result); return result;
      }
      const binding: TargetBinding = Object.freeze({
        kind: "worker", target_ref: ref, task_id: snapshot.value.identity.task_id,
        run_epoch: snapshot.value.identity.run_epoch, generation,
        owner_session_id: rootBinding.owner_session_id, owner_root_id: rootBinding.owner_root_id,
        canonical_session_id: hello.value.session_id, session_id: normalize(hello.value.session_id),
        runtime_instance_id: hello.value.runtime_instance_id,
      });
      remote.set(ref, { binding, wire: hello.wire, handle: new WeakRef(native) });
      void hello.wire.finished.promise.then(() => options.onRemoteEvent?.(binding, { kind: "closed" }));
      ticket.used = true; ticket.target = ref;
      const result = { ok: true as const, value: { binding, root_ref: rootBinding.target_ref } };
      ticket.approved.resolve(result); refresh();
      return result;
    },
    diagnostics() {
      return { listeners: listeners.size, endpoints: (server.listening ? 1 : 0) + remote.size,
        pending_requests: pending + [...sockets].reduce((sum, wire) => sum + wire.pendingCount, 0),
        frames: [...sockets].reduce((sum, wire) => sum + wire.frames, 0) };
    },
    dispose,
    handlers: options.handlers,
  };
  return { ok: true as const, value: Object.freeze(channel) };
}
export type PeerChannel = Extract<Awaited<ReturnType<typeof createPeerChannel>>, { ok: true }>["value"];

/** Runs only in the observed native child. No SDK import or second capture owner. */
export async function connectRemoteRuntime(options: {
  invitation: RemoteInvitation; capture: CapturedRuntime; context: Context; handlers?: PeerHandlers;
  onDisconnect?: () => void;
}) {
  const runtime = options.capture.runtime();
  const snapshot = options.capture.read();
  if (!runtime || !snapshot.ok || runtime.sessionManager !== options.context.sessionManager ||
      !options.invitation.address.startsWith("\\\\.\\pipe\\omo-coordination-")) return refuse("scope_denied");
  const socket = createConnection(options.invitation.address);
  const wire = new Wire(socket, async (message, signal) => {
    const fresh = options.capture.read();
    const call = message?.call as PeerCall | undefined;
    if (!fresh.ok || options.capture.runtime() !== runtime || message?.kind !== "invoke" ||
        call?.target.runtime_instance_id !== options.capture.runtime_instance_id ||
        normalize(call.target.canonical_session_id) !== normalize(runtime.sessionId) ||
        call.target.generation !== options.invitation.generation) return refuse("stale_generation");
    const handler = options.handlers?.[call.operation];
    return handler ? handler({ ...call, signal }) : refuse("coordination_unavailable");
  });
  await bounded(new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve); socket.once("error", reject);
  }), "pipe connect");
  const accepted = await bounded(wire.call({ kind: "hello", hello: {
    token: options.invitation.token, generation: options.invitation.generation, pid: process.pid,
    session_id: runtime.sessionId, runtime_instance_id: options.capture.runtime_instance_id,
    branch: snapshot.value.branch,
  } }), "native owner approval");
  if (!accepted.ok) { wire.close(); return accepted; }
  const value = accepted.value as { binding: TargetBinding; root_ref: TargetRef };
  void wire.finished.promise.then(() => options.onDisconnect?.());
  return { ok: true as const, value: Object.freeze({
    binding: value.binding, root_ref: value.root_ref,
    isCurrent: () => !wire.isClosed && options.capture.runtime() === runtime && options.capture.alive(),
    emit(event: unknown) { return wire.call({ kind: "event", value: event }); },
    onClose(listener: () => void) {
      let active = true;
      void wire.finished.promise.then(() => { if (active) listener(); });
      return () => { active = false; };
    },
    request(request: PeerRequest) {
      const { signal, ...input } = request;
      return wire.call({ kind: "route", request: input }, signal);
    },
    diagnostics: () => ({ pending_requests: wire.pendingCount, frames: wire.frames }),
    dispose: () => { wire.close(); },
  }) };
}
export type RemoteRuntime = Extract<Awaited<ReturnType<typeof connectRemoteRuntime>>, { ok: true }>["value"];

/** The owning extension publishes once per generation. Missing handlers stay unavailable. */
export function publishCoordinationBroker(
  channel: PeerChannel,
  options: { host?: object; deepEnabled?: boolean; repairEnabled?: boolean } = {},
) {
  const host = (options.host ?? globalThis) as Record<symbol, unknown>;
  const key = Symbol.for("omo.agent-coordination.api.v1");
  if (host[key] !== undefined) throw new Error("COORDINATION_BROKER_ALREADY_REGISTERED");
  const features: Feature[] = [];
  if (options.deepEnabled && channel.handlers?.ask) features.push("ask_depth_deep");
  if (channel.handlers?.setAskPolicy) features.push("ask_policy");
  if (channel.handlers?.inspect) features.push("inspect");
  if (options.repairEnabled && process.env.OMO_AGENT_COORDINATION_REPAIR !== "0" &&
      channel.handlers?.revival_settlement_repair) features.push("revival_settlement_repair");
  let active = true;
  const clients = new Set<() => void>();
  const broker: CoordinationBroker = Object.freeze({
    version: 1, features: Object.freeze(features),
    getClient(ctx: unknown): Result<CoordinationClient> {
      if (!active) return { ok: false, reason: "stale_generation" };
      const caller = channel.bindContext(ctx);
      if (!caller.ok) return caller;
      let disposed = false;
      const subscriptions = new Set<() => void>();
      const current = (): Result<TargetBinding> => !active || disposed
        ? { ok: false, reason: "stale_generation" } : channel.bindContext(ctx);
      const request = (operation: string, input: any): Promise<any> => {
        const valid = current();
        if (!valid.ok) return Promise.resolve(valid);
        const { signal, ...payload } = input;
        return channel.request(ctx, { operation, payload, signal,
          target_ref: input.target_ref, request_id: input.request_id ?? randomUUID() });
      };
      const dispose = () => {
        if (disposed) return;
        disposed = true;
        for (const stop of subscriptions) stop();
        subscriptions.clear(); clients.delete(dispose);
      };
      clients.add(dispose);
      return { ok: true, value: Object.freeze({
        peers: () => { const valid = current(); return valid.ok ? channel.peers() : valid; },
        ask: (input) => typeof input?.request_id !== "string" || !input.request_id
          ? Promise.resolve({ ok: false, reason: "invalid_argument" }) : request("ask", input),
        notify: (input) => typeof input?.request_id !== "string" || !input.request_id
          ? Promise.resolve({ ok: false, reason: "invalid_argument" }) : request("notify", input),
        inspect: (target_ref) => request("inspect", { target_ref }),
        setAskPolicy: (input) => {
          const valid = current();
          return valid.ok ? channel.setAskPolicy(ctx, input) : valid;
        },
        onPeersChanged(listener) {
          if (!current().ok) return () => {};
          const stop = channel.onPeersChanged((roster) => { if (current().ok) listener(roster); });
          subscriptions.add(stop);
          return () => { stop(); subscriptions.delete(stop); };
        },
        dispose,
      } satisfies CoordinationClient) };
    },
  });
  host[key] = broker;
  return Object.freeze({ broker, dispose() {
    if (!active) return;
    active = false;
    for (const dispose of [...clients]) dispose();
    if (host[key] === broker) delete host[key];
  } });
}
