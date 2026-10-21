# Disposable candidate installation

Run from the product root with Bun. This candidate installer requires an explicit
agent directory strictly below the fixed attempt directory:

`.omo/evidence/agent-coordination/attempt-5138-rebuild-e1/`

It refuses the live profile and paths or ancestor junctions/symlinks that escape
this boundary. It does not copy authentication, user sessions, model settings,
the native engine or SDK into the package. Live installation needs separate
approval after final verification; this tool intentionally does not offer a
live-install override.

```powershell
bun scripts/install.mjs install --agent-dir "<attempt>/task-9/demo/agent" --dry-run
bun scripts/install.mjs install --agent-dir "<attempt>/task-9/demo/agent"
bun scripts/install.mjs uninstall --agent-dir "<attempt>/task-9/demo/agent" --dry-run
bun scripts/install.mjs uninstall --agent-dir "<attempt>/task-9/demo/agent"
```

Dry-run validates inputs and prints a machine-readable plan without writing,
building files on disk, loading the extension or contacting a provider.
Installation builds one ESM entry and a separate pure API helper, includes their
TypeScript contracts and documentation, and adds only its package path to
settings.packages. Existing extensions, roles, models and task state are preserved.
The resulting package's pi.extensions is `["./dist/extension.js"]`; the native
package resolver can propagate that directly loaded .js entry to children.

## Optional managed native adapter

Copy the prepared native OMO task bundle into a disposable fixture first, then
pass that copy explicitly:

```powershell
bun scripts/install.mjs install --agent-dir "<attempt>/task-9/demo/agent" --adapter-target "<attempt>/task-9/demo/agent/native/omo-task.js"
```

The target must be inside agentDir and match one of the pinned baseline digests
in `scripts/build-adapter.mjs`: 5.1.38
`36d98c7ab3cc07349b60adc8bc7365a85e7573653ebab574d0114c2571d53dcd`
(1,100,451 B) or 5.1.40
`d41bc807b6f91768db84b7f8f76092107aebf9934888cc749174d88a1c00ac6b`
(1,105,299 B). Unknown bundle bytes are refused before any output is written.
Each baseline has its own anchor table and SDK pin file.
The builder also verifies pinned AgentSession SDK bytes and unique structural
anchors. Its manifest records each exact before/after anchor, bootstrap,
baseline/candidate hashes and why the native owner-engine connection is required.
The disposable native fixture must load this adapted target; merely installing
the collaboration entry cannot grant owner-engine authority.

The delta connects only the task component engine, original outcome ports,
actual task SDK readiness and exact runner-owned runtime. It is inert without
the subscribing mode. It is not a second scheduler or a vendored engine.
Source version: omo-ai 5.1.38, original semantic references
`d7e224e478a4a77dba0d2104c336fdc72ad301e3`. See scripts/build-adapter.mjs and
the generated manifest for the complete delta.

## 5.1.40 / Senpi 2026.10.19-2 adapter (applied 2026-10-22)

Branch `port/omo-5.1.40` builds the adapter from the stock 5.1.40 bundle
`d41bc807...` with a separate twelve-anchor table. The rebuild of the preserved
5.1.38 bundle is still byte-identical to the recorded candidate
`2b559bb11f0285072722cffbb7f322756c81aec867f6fb4a683278f075d7f783`.
On 2026-10-22 the stable-500 lane switched to this pair. `dist/extension.js`
(`6035ef1a...`) and the plain adapter `c2269e28...` with the plan-review-unlock
overlay (`0dd146b9...`) were copied over the package and both `omo-task.js` copies
by a hash-guarded script that keeps the stock backups.
The 5.1.40 plain adapter and its plan-review-unlock overlay, the senpi 19 vs 19-2
comparison, the test log and the guarded live apply plan are recorded outside the
repository in
`.omo/evidence/omo-maintenance-20261021/m5/coordination/REPORT.md`.

## Live adapter installation (5.1.38 candidate, NOT applied)

The 2026-10-19 rebuild targets omo-ai 5.1.38 / Senpi 2026.10.19.
Evidence and hash-guarded scripts:

`.omo/evidence/agent-coordination/attempt-5138-rebuild-e1/`

Baseline SHA-256: `36d98c7ab3cc07349b60adc8bc7365a85e7573653ebab574d0114c2571d53dcd`
(1,100,451 bytes). Candidate SHA-256:
`2b559bb11f0285072722cffbb7f322756c81aec867f6fb4a683278f075d7f783`.
The candidate has 12 unique anchors and freshly measured AgentSession SDK pins.
See `RESULT.json` for sizes, test results, raw evidence, and exact apply commands.
The repository distribution was rebuilt; the installed coordination package was
not changed. The source's 1000 ms per-bridge owned-list cache is preserved.

Both live targets remain stock baseline bytes:

| Target | Suggested new baseline backup |
| --- | --- |
| `C:/Users/USER/.omo-stable-500/staging/memory-rollback-5.1.38-r1/package/plugin/extensions/omo-task.js` | `<attempt>/live-install/backup/omo-task.js.5138-baseline-overlay` |
| `C:/Users/USER/.bun/install/global/node_modules/omo-ai/plugin/extensions/omo-task.js` | `<attempt>/live-install/backup/omo-task.js.5138-baseline-global` |

After review, run the following once per target, overlay first. Substitute the
absolute attempt directory and the corresponding target/backup from the table.

```powershell
node "<attempt>/live-install/apply-live-install.mjs" --target "<target>" --candidate "<attempt>/candidate/omo-task.js" --backup "<backup>" --dry-run
node "<attempt>/live-install/apply-live-install.mjs" --target "<target>" --candidate "<attempt>/candidate/omo-task.js" --backup "<backup>"
node "<attempt>/live-install/remove-live-install.mjs" --target "<target>" --backup "<backup>" --dry-run
node "<attempt>/live-install/remove-live-install.mjs" --target "<target>" --backup "<backup>"
```

Apply refuses candidate or target drift and an existing backup path; remove
refuses target or backup drift. Both use an atomic same-directory replacement
and re-hash the result. Removal preserves the backup: re-apply needs a fresh
backup path. Apply/remove/re-apply is rehearsed only on disposable copies of both
targets. Neither script has been run against a live target for this rebuild.

The overlay is the copy stable-500 loads. New sessions load changed bundles;
existing sessions retain their loaded copy. `OMO_AGENT_COORDINATION=0` disables
capture and tools on reload. Use this attempt's removal script and its own
5.1.38 backups, not the retained 5.1.34/5.1.35 installer state or backups.
The generic installer remains disposable-only and is not the live removal path.

## Fail-closed uninstall and rollback

`rollback` uses exactly the same guarded restoration path as `uninstall`.
Before deleting or overwriting anything, both validate:

- the receipt and its SHA-256 seal, agent/package identity and exact package file inventory;
- every installed package file and current settings bytes;
- original settings backup, when settings originally existed;
- current adapted target, native baseline backup and adapter manifest, when used.

Missing receipts, missing or corrupt backups, changed installed files, changed
settings, unexpected package files and symlink paths exit non-zero before any
mutation. Preserve the evidence and resolve the drift deliberately; do not
delete the installed entry as a workaround. Removing receipt.sha256 does not
make an installation unmanaged or safe to remove.

On a valid removal, original settings bytes and optional native target bytes
are restored, and only the inventoried package and its dedicated install state
are removed. Other packages, extensions and task records are untouched. Reload
the disposable session after changing its package registration.

The extension cancels only its side requests, drops only its unconsumed
generation-owned next-turn messages, removes its broker/listeners and leaves
old clients stale. Main work is not aborted or revived. Repair can be disabled
independently using OMO_AGENT_COORDINATION_REPAIR=0; full disable uses
OMO_AGENT_COORDINATION=0. Both switches take effect at the next generation.

## Provenance and licensing

This repository's original coordination implementation and small adapter delta
are covered by LICENSE. No full upstream bundle, SDK implementation, credential
or raw session transcript is part of the distributable package.

The installed Senpi package declares MIT, author Mario Zechner, repository
code-yeongyu/senpi. The installed omo-ai/plugin/LICENSE grants MIT specifically
for the named Senpi LSP adapter descriptor/schema/renderer/path extraction/
post-edit wiring/migration-warning portions (copyright 2026 Yeongyu Kim); it is
not evidence of a blanket MIT grant for the entire native task engine. Native
input bytes are therefore used locally for authorized fixture validation only
and are not redistributed by this package. Preserve each dependency's own
notices when independently distributing dependencies.

GJC v0.18.11 commit `9a77bb3f3901324c85c6d080da0e4120b1cd2308` supplied
design references for ephemeral replies, peer rosters and next-turn delivery;
its implementation was not transplanted. Existing summary, publication-retry,
no-fallback and Herdr extensions are loaded from their actual local code for
coexistence QA, not copied into the product or repaired here.
