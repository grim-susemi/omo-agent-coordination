import { afterEach, expect, test } from "bun:test";
import { CARRIER_MISSING_WARNING, warnIfCarrierMissing } from "../src/startup-warning.ts";

const CARRIER = Symbol.for("omo.agent-coordination.native.v1");
const root = globalThis as unknown as Record<symbol, unknown>;
const saved = root[CARRIER];
const savedMode = process.env.OMO_AGENT_COORDINATION;
afterEach(() => {
  if (saved === undefined) delete root[CARRIER]; else root[CARRIER] = saved;
  if (savedMode === undefined) delete process.env.OMO_AGENT_COORDINATION; else process.env.OMO_AGENT_COORDINATION = savedMode;
});
const session = () => {
  const lines: string[] = [];
  return { lines, context: { sessionManager: {}, ui: { notify: (m: string) => { lines.push(m); } } } };
};
const ns = {} as never;

test("absent carrier warns exactly once per session, even on repeated session_start", () => {
  delete root[CARRIER];
  const a = session(), b = session();
  warnIfCarrierMissing(a.context, ns, 1); warnIfCarrierMissing(a.context, ns, 1);
  warnIfCarrierMissing(b.context, ns, 1);
  expect(a.lines).toEqual([CARRIER_MISSING_WARNING]);
  expect(b.lines).toEqual([CARRIER_MISSING_WARNING]);
});

test("mismatched baseline warns; disabled mode stays silent", () => {
  root[CARRIER] = { version: 2, baseline_sha256: "bad", contexts: new WeakMap() };
  const a = session(); warnIfCarrierMissing(a.context, ns, 1);
  expect(a.lines).toEqual([CARRIER_MISSING_WARNING]);
  delete root[CARRIER]; process.env.OMO_AGENT_COORDINATION = "0";
  const b = session(); warnIfCarrierMissing(b.context, ns, 1);
  expect(b.lines).toEqual([]);
});
