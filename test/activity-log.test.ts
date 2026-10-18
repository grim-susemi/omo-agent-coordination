import { expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ACTIVITY_FILE, ACTIVITY_MAX_BYTES, appendActivity, collabLine, showCollab } from "../src/activity-log.ts";

const temp = () => fs.mkdtempSync(path.join(os.tmpdir(), "coordination-activity-"));
const lines = (dir: string) => fs.readFileSync(path.join(dir, ACTIVITY_FILE), "utf8").split("\n").filter(Boolean)
  .map((line) => JSON.parse(line));

test("one schema line per event under the resolved agent dir, summaries single-line and bounded", () => {
  const dir = temp();
  try {
    expect(appendActivity(path.join(dir, "agent"), { kind: "ask", phase: "request", id: "q-1", from: "root",
      to: "worker-a", summary: "line one\nline two\r\u0007" + "x".repeat(300), ok: null, mode: "quick" })).toBe(true);
    expect(appendActivity(path.join(dir, "agent"), { kind: "ask", phase: "reply", id: "q-1", from: "worker-a",
      to: "root", summary: "done", ok: true, latency_ms: 3000.4, mode: "quick" })).toBe(true);
    const [request, reply] = lines(path.join(dir, "agent"));
    expect(request).toMatchObject({ v: 1, kind: "ask", phase: "request", id: "q-1", from: "root", to: "worker-a",
      ok: null, mode: "quick" });
    expect(Number.isFinite(Date.parse(request.ts)) && request.ts.endsWith("Z")).toBe(true);
    expect(request.summary).not.toMatch(/[\r\n\u0007]/);
    expect(Array.from(request.summary as string).length).toBe(160);
    expect(reply).toMatchObject({ kind: "ask", phase: "reply", ok: true, latency_ms: 3000 });
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("an unwritable agent dir is dropped without throwing", () => {
  const dir = temp(), blocker = path.join(dir, "file-not-dir");
  try {
    fs.writeFileSync(blocker, "x");
    expect(appendActivity(blocker, { kind: "notify", from: "a", to: "b", summary: "n", ok: true })).toBe(false);
    expect(appendActivity(undefined, { kind: "notify", from: "a", to: "b", summary: "n", ok: true })).toBe(false);
    expect(showCollab({ notify() { throw new Error("ui gone"); } }, "x")).toBe(false);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("a file over the size bound is rotated to .1 before the next append", () => {
  const dir = temp(), file = path.join(dir, ACTIVITY_FILE);
  try {
    fs.writeFileSync(file, "x".repeat(ACTIVITY_MAX_BYTES + 1));
    expect(appendActivity(dir, { kind: "inspect", phase: "done", from: "root", to: null, summary: "visited 1/1", ok: true })).toBe(true);
    expect(fs.statSync(file + ".1").size).toBe(ACTIVITY_MAX_BYTES + 1);
    expect(lines(dir)).toHaveLength(1);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("session lines follow the v4 shapes", () => {
  expect(collabLine.askSent("리드", "워커A", "레인2 상태 어때?")).toBe("▸ 협업  리드 → 워커A  \"레인2 상태 어때?\"  ⏳ 대기");
  expect(collabLine.askReply("워커A", "리드", true, "빌드 중, 3분 남음", 3000, "quick"))
    .toBe("▸ 협업  워커A → 리드  \"빌드 중, 3분 남음\"  ✅ 3s (quick)");
  expect(collabLine.asked("리드", "레인2 상태 어때?", "quick")).toBe("▸ 협업  리드가 물음 — \"레인2 상태 어때?\" (quick, 답 1회)");
  expect(collabLine.replied("리드", "빌드 중, 3분 남음")).toBe("▸ 협업  답 보냄 → 리드  \"빌드 중, 3분 남음\"  ✅");
  expect(collabLine.notified("리드", "워커B", true)).toBe("▸ 협업  리드 → 워커B  노트 전달(중단 없음)  📝 다음 턴");
});
