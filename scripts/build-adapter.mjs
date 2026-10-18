import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { attempt, bundle, installedBundle } from "../qa/command.mjs";

export const BASELINE = "eb2db2c195edcf8e42dac8944535b7df85ad6e90fe1c806676573296799b2287";
export const CARRIER = "omo.agent-coordination.native.v1";
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const key = `globalThis[Symbol.for("${CARRIER}")]`;

// This carrier holds only weak engine references. Ports are keyed by their native store,
// not by a caller's task/session string. It remains inert without a subscribing mode.
const bootstrap = `
var __coordNative=${key}??={version:2,baseline_sha256:"${BASELINE}",
engines:new Set,links:new WeakMap,ports:new WeakMap,contexts:new WeakMap,generations:new WeakMap,
runtimeOwners:new WeakMap,
reviewExecutions:new WeakMap,
independentReview(engine,spec){const link=this.links.get(engine),normal=id=>String(id??"").replace(/^senpi:/,"");
if(!link?.active||!spec||normal(spec.parent_session_id)!==normal(engine.runtime.sessionId())||
normal(spec.root_session_id??spec.parent_session_id)!==normal(engine.ancestry?.rootSessionId??engine.runtime.sessionId()))
throw Error("INDEPENDENT_REVIEW_OWNER_MISMATCH");
this.reviewExecutions.set(spec,new WeakRef(engine.manager))},
reviewPlan(manager,spec,plan){if(this.reviewExecutions.get(spec)?.deref()!==manager)return plan;
this.reviewExecutions.delete(spec);
return{...plan,toolDenylist:[...new Set([...(plan.toolDenylist??[]),"coordination_ask","coordination_notify","__coordination_independent_review__"]) ]}},
moduleListeners:new Set,module:void 0,
terminalObservers:new WeakMap,replays:new WeakMap,
outcomePorts(ports){this.ports.set(ports.store,ports)},
moduleReady(module){this.module=module;for(const listener of this.moduleListeners)listener(module)},
engine(engine,store,pi){const ref=new WeakRef(engine);this.engines.add(ref);
const link={store,active:true};this.links.set(engine,link);
pi.on("session_start",(_event,ctx)=>{if(ref.deref()){link.active=true;this.contexts.set(ctx.sessionManager,ref)}});
pi.on("session_shutdown",()=>{link.active=false;this.engines.delete(ref)});
for(const stale of this.engines)if(!stale.deref())this.engines.delete(stale)}};
`;

export const DELTA = Object.freeze([
  {
    seam: "R6: consume an owner-registered execution policy inside the original native launch",
    before: "async#se(e,t,n,r){let i=t.maxDepth",
    after: `async#se(e,t,n,r){t=${key}.reviewPlan(this,e,t);let i=t.maxDepth`,
  },
  {
    seam: "R6: composeTaskEngine return, same engine object and store",
    before: "return t=T,{manager:T,resolveInheritedExtensions:b,lifecycle:k,",
    after: `return t=T,((__coordEngine)=>{${key}.engine(__coordEngine,v.store,e.pi);return __coordEngine})({manager:T,resolveInheritedExtensions:b,lifecycle:k,`,
  },
  {
    seam: "R6: close the exact engine return expression",
    before: "onStoreMutation:v.onMutation}}function E5(",
    after: "onStoreMutation:v.onMutation})}function E5(",
  },
  {
    seam: "R7: createOutcomeTracker receives the original manager closures",
    before: "function BH(e){let t=function(e){",
    after: `function BH(e){${key}.outcomePorts(e);(${key}.replays??=new WeakMap).set(e.store,(...a)=>n(...a));let t=function(e){`,
  },
  {
    seam: "R7: observe the native chosen terminal transition and preserve native failure cleanup",
    before: 'function n(t,n,r,i){try{e.store.transition(t,i)}catch(i){return Fm("senpi-task manager terminal record write failed"',
    after: `function n(t,n,r,i){let __coordAttempt=${key}.terminalObservers?.get(e.store)?.(t,n,i);try{e.store.transition(t,i)}catch(i){__coordAttempt?.failed(i);return Fm("senpi-task manager terminal record write failed"`,
  },
  {
    seam: "R7: replay-only fence in the native reducer, inside the original store record lock",
    before: 'function ey(e,t){if("cancel"===t.type',
    after: 'function ey(e,t){let __coordFence=t[Symbol.for("omo.agent-coordination.terminal-fence.v1")];if(__coordFence&&!__coordFence(e))return{applied:!1,record:e,audit:{type:"coordination_replay_fenced"}};if("cancel"===t.type',
  },
  {
    seam: "R13: awaited lazy barrel readiness, before child construction",
    before: 'function Os(){let e=As();return e.promise??=import("@code-yeongyu/senpi").then(t=>(e.module=t,t)),e.promise}',
    after: `function Os(){let e=As();if(e.module!==void 0)${key}.moduleReady(e.module);return e.promise??=import("@code-yeongyu/senpi").then(t=>(e.module=t,${key}.moduleReady(t),t)),e.promise}`,
  },
  {
    seam: "R8: native in-process turn factory owns the exact AgentSession parameter",
    before: "s.then(()=>{o=!1},()=>{o=!1})};return{handle:{task_id:e,sessionId:t.sessionId,",
    after: `s.then(()=>{o=!1},()=>{o=!1})};return((__coordTurn)=>{${key}.runtimeOwners.set(__coordTurn.handle,{task_id:e,runtime:new WeakRef(t)});return __coordTurn})({handle:{task_id:e,sessionId:t.sessionId,`,
  },
  {
    seam: "R8: close native in-process turn factory capture",
    before: "await lF(t))}},beginTurn:l}}async function dF(e)",
    after: "await lF(t))}},beginTurn:l})}async function dF(e)",
  },
  {
    seam: "R6/R8: preserve runtime ownership through the native managed-handle conversion",
    before: "function hW(e){return{task_id:e.task_id,kind:\"in-process\",sessionId:e.sessionId,pid:void 0,effectiveModel:()=>e.effectiveModel(),steer:t=>e.steer(t),followUp:t=>e.followUp(t),abort:()=>e.abort(),subscribe:t=>e.subscribe(t),waitForOutcome:()=>e.waitForIdle(),lastAssistantText:()=>e.lastAssistantText(),dispose:async()=>{await e.dispose()}}}",
    after: `function hW(e){let t={task_id:e.task_id,kind:"in-process",sessionId:e.sessionId,pid:void 0,effectiveModel:()=>e.effectiveModel(),steer:t=>e.steer(t),followUp:t=>e.followUp(t),abort:()=>e.abort(),subscribe:t=>e.subscribe(t),waitForOutcome:()=>e.waitForIdle(),lastAssistantText:()=>e.lastAssistantText(),dispose:async()=>{await e.dispose()}};let n=${key}.runtimeOwners.get(e);return void 0!==n&&${key}.runtimeOwners.set(t,n),t}`,
  },
  {
    seam: "R8: read session identity through the original RPC client, never launcher PID metadata",
    before: "getEntries:e=>o.getEntries(e)})",
    after: 'getEntries:e=>o.getEntries(e),[Symbol.for("omo.agent-coordination.rpc-identity.v1")]:async()=>{let e=await o.send({type:"get_state"});if(!e.success||e.command!=="get_state")throw Error("RPC_IDENTITY_UNAVAILABLE");return{session_id:e.data.sessionId}}})',
  },
  {
    seam: "R6/R8: preserve the private read-only RPC identity port on its original managed handle",
    before: "getEntries:e=>n(e)},lastAssistantText:",
    after: 'getEntries:e=>n(e)},...void 0===e[Symbol.for("omo.agent-coordination.rpc-identity.v1")]?{}:{[Symbol.for("omo.agent-coordination.rpc-identity.v1")]:e[Symbol.for("omo.agent-coordination.rpc-identity.v1")]},lastAssistantText:',
  },
]);

function candidatePath(file) {
  const absolute = path.resolve(file);
  const relative = path.relative(attempt, absolute);
  if (!relative || path.isAbsolute(relative) || relative.startsWith("..")) {
    throw new Error("CANDIDATE_OUTSIDE_ATTEMPT");
  }
  let parent = path.dirname(absolute);
  while (!fs.existsSync(parent)) parent = path.dirname(parent);
  const resolved = fs.realpathSync(parent);
  const parentRelative = path.relative(fs.realpathSync(attempt), resolved);
  if (path.isAbsolute(parentRelative) || parentRelative.startsWith("..")) {
    throw new Error("CANDIDATE_PARENT_ESCAPES_ATTEMPT");
  }
  if (fs.existsSync(absolute) && fs.lstatSync(absolute).isSymbolicLink()) {
    throw new Error("CANDIDATE_IS_SYMLINK");
  }
  return absolute;
}

export function buildAdapter({ input = bundle, output }) {
  output = candidatePath(output);
  if (path.resolve(input) === output) throw new Error("INPUT_IS_OUTPUT");
  const original = fs.readFileSync(input);
  if (digest(original) !== BASELINE) throw new Error("BUNDLE_DRIFT_REFUSED");
  const pins = JSON.parse(fs.readFileSync(path.join(attempt, "task-1/source-pins.json"), "utf8"));
  const sdkPins = pins.files.filter((file) => /agent-session\.(?:js|d\.ts)$/.test(file.path));
  for (const pin of sdkPins) {
    if (digest(fs.readFileSync(pin.path)) !== pin.sha256) throw new Error("SDK_DRIFT_REFUSED");
  }
  let text = original.toString();
  const anchors = DELTA.map((delta) => {
    const count = text.split(delta.before).length - 1;
    if (count !== 1) throw new Error(`ANCHOR_NOT_UNIQUE: ${delta.seam}: ${count}`);
    const offset = text.indexOf(delta.before);
    text = text.replace(delta.before, delta.after);
    return { ...delta, offset, count };
  });
  text = bootstrap + text;
  const manifest = {
    adapter_version: 2, input: path.resolve(input), output, baseline_sha256: BASELINE,
    candidate_sha256: digest(text), sdk_sha256: Object.fromEntries(sdkPins.map((p) => [p.path, p.sha256])),
    anchors, bootstrap, reason: "No public SDK port exposes the live TaskComponent engine, OutcomeTrackerPorts, or the exact AgentSession owned by its managed child handle.",
    removal: { backup: output + ".baseline", expected_sha256: BASELINE, hash_guarded: true },
    mode_absent: "Weak discovery only; no session/handle/SDK copy, mutation, or native outcome wrapping.",
    runtime_ownership: {
      capture: "R8 aF(task_id, session, completion): exact session parameter, keyed by the returned runner handle",
      propagation: "R6/R8 hW(runnerHandle): the same ownership entry, keyed by the returned ManagedChildHandle",
      validation: "Same live native manager/outcome handle and current task/epoch; runtime object identity, never constructor or session-ID membership",
      references: "WeakMap handle keys and WeakRef AgentSession values; native lifetime remains unchanged",
    },
    source: "omo-ai 5.1.35 installed bundle; original semantic references d7e224e478a4a77dba0d2104c336fdc72ad301e3 (R6/R7/R8/R13)",
    license: "Installed plugin LICENSE and NOTICE apply to the input; only this small delta is product source.",
  };
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output + ".baseline", original, { flag: "wx" });
  fs.writeFileSync(output, text, { flag: "wx" });
  fs.writeFileSync(output + ".manifest.json", JSON.stringify(manifest, null, 2) + "\n", { flag: "wx" });
  // Resolve imports to the installed modules' real paths, not a second SDK installation.
  const modules = path.join(path.dirname(output), "node_modules");
  if (!fs.existsSync(modules)) fs.symlinkSync(
    path.resolve(path.dirname(installedBundle), "../../.."), modules, "junction");
  return manifest;
}

export function removeAdapter(manifest) {
  const output = candidatePath(manifest.output);
  if (digest(fs.readFileSync(output)) !== manifest.candidate_sha256) throw new Error("REMOVAL_DRIFT_REFUSED");
  const baseline = fs.readFileSync(candidatePath(manifest.removal.backup));
  if (digest(baseline) !== BASELINE) throw new Error("BACKUP_DRIFT_REFUSED");
  fs.writeFileSync(output, baseline);
  return { restored_sha256: digest(fs.readFileSync(output)), matches_baseline: true };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const output = process.argv[process.argv.indexOf("--output") + 1];
  if (!process.argv.includes("--output") || !output) throw new Error("--output is required");
  console.log(JSON.stringify(buildAdapter({ output })));
}
