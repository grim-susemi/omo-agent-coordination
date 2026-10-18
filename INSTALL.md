# Disposable candidate installation

Run from the product root with Bun. This candidate installer requires an explicit
agent directory strictly below the fixed attempt directory:

`.omo/evidence/agent-coordination/attempt-5135-rebuild-e1/`

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

The target must be inside agentDir and match SHA-256
`eb2db2c195edcf8e42dac8944535b7df85ad6e90fe1c806676573296799b2287`
(omo-ai 5.1.35, 1,096,682 B).
The builder also verifies pinned AgentSession SDK bytes and unique structural
anchors. Its manifest records each exact before/after anchor, bootstrap,
baseline/candidate hashes and why the native owner-engine connection is required.
The disposable native fixture must load this adapted target; merely installing
the collaboration entry cannot grant owner-engine authority.

The delta connects only the task component engine, original outcome ports,
actual task SDK readiness and exact runner-owned runtime. It is inert without
the subscribing mode. It is not a second scheduler or a vendored engine.
Source version: omo-ai 5.1.35, original semantic references
`d7e224e478a4a77dba0d2104c336fdc72ad301e3`. See scripts/build-adapter.mjs and
the generated manifest for the complete delta.

## Live adapter installation (overlay + global, applied 2026-10-18)

The live bundle copies are patched in place by the hash-guarded pair
`apply-live-install.mjs` / `remove-live-install.mjs`, kept next to this attempt's
evidence:

`.omo/evidence/agent-coordination/attempt-5135-rebuild-e1/live-install/`

```powershell
node "<attempt>/live-install/apply-live-install.mjs"  --target "<target>" --candidate "<attempt>/candidate/omo-task.js" --backup "<backup>" --dry-run
node "<attempt>/live-install/apply-live-install.mjs"  --target "<target>" --candidate "<attempt>/candidate/omo-task.js" --backup "<backup>"
node "<attempt>/live-install/remove-live-install.mjs" --target "<target>" --backup "<backup>" --dry-run
node "<attempt>/live-install/remove-live-install.mjs" --target "<target>" --backup "<backup>"
```

Baseline (restored by `remove`) SHA-256
`eb2db2c195edcf8e42dac8944535b7df85ad6e90fe1c806676573296799b2287` (1,096,682 B);
candidate SHA-256 `0dcf892f1d3ff3f1c20845516a7fed03c3b93c043e3b8bac9dffbae8f79110a6`
(1,100,069 B). Both targets were applied on 2026-10-18 and re-measured on
2026-10-18.

| Target | sha256 now | Backup (baseline bytes) |
| --- | --- | --- |
| `C:/Users/USER/.omo-stable-500/staging/memory-rollback-5.1.35-r1/package/plugin/extensions/omo-task.js` | `0dcf892f1d3ff3f1c20845516a7fed03c3b93c043e3b8bac9dffbae8f79110a6` | `C:/Users/USER/.omo-stable-500/home/.omo/agent/.agent-coordination-install/adapter/omo-task.js.5135-baseline-overlay` = `eb2db2c195edcf8e42dac8944535b7df85ad6e90fe1c806676573296799b2287` |
| `C:/Users/USER/.bun/install/global/node_modules/omo-ai/plugin/extensions/omo-task.js` | `0dcf892f1d3ff3f1c20845516a7fed03c3b93c043e3b8bac9dffbae8f79110a6` | `C:/Users/USER/.omo-stable-500/home/.omo/agent/.agent-coordination-install/adapter/omo-task.js.5135-baseline-global` = `eb2db2c195edcf8e42dac8944535b7df85ad6e90fe1c806676573296799b2287` |

The overlay copy is the one the stable-500 lane actually loads; the global copy
is kept in sync. Both scripts are fail-closed and write before any change only
after every check passes:

- `apply`: refuses unless the candidate file hashes to the candidate above, the
target currently hashes to the baseline above, and the backup path does not
exist yet; writes the backup with `wx`, then writes the target through a
temporary file in the same directory, renames it and re-hashes both files.
- `remove`: refuses unless the target hashes to the candidate and the backup to
the baseline, then restores the backup bytes through the same atomic path.
- Drift on either side exits non-zero before any write. A byte-flipped backup
yields `BACKUP_DRIFT_REFUSED` and leaves the target untouched (rehearsed on a
disposable copy: apply, real remove restoring baseline bytes, re-apply).
- Backups live in the agent install state
  (`<agent>/.agent-coordination-install/adapter/…-5135-baseline-{overlay,global}`)
  with a second copy under this attempt's `live-install/backup/`.
- A bundle is read at session start: a change reaches new sessions only, and an
already open session keeps the old copy. `OMO_AGENT_COORDINATION=0` still
disables capture and the coordination tools on reload.

The install-state `receipt.json` is the final, sealed receipt (`draft:false`,
`sealed:true`; `receipt.sha256` is the SHA-256 of its bytes). It records both
targets in `adapters[]` with their baseline, candidate, pre and post hashes and
both backup paths. Its `adapter.manifest_sha256` is the manifest of the installed
5.1.35 candidate,
`.omo/evidence/agent-coordination/attempt-5135-rebuild-e1/candidate/omo-task.js.manifest.json`
(`a67e5c1cdd1a1b845e569433accb9631ed09071ed1e4a4f4a9677052d51f7084`). The
previous value, the retained 5.1.34 manifest `adapter/omo-task.js.manifest.json`
(`fdcc7e842765b28a63fdf2f30914a9b54f2f9cc4dd3492af989acbb6f04a9635`), is kept
under `adapter.previous.manifest_sha256`.

The retained 5.1.34 generic-installer state under
`<agent>/.agent-coordination-install/adapter/` (`omo-task.js`,
`omo-task.js.baseline`, the manifest above) is not the 5.1.35 baseline. Run as is, `node scripts/install.mjs uninstall`
stops earlier at its scope guards (`AGENT_DIR_REQUIRED` without an agent
directory, `DISPOSABLE_AGENT_DIR_REQUIRED` against the live one). On an
admissible disposable fixture with the receipt identity and the adapter target
relocated, it reaches the retained-backup guard and refuses with
`ADAPTER_BACKUP_REFUSED`, changing nothing. Do not use it to remove the live adapter. Remove the live
adapter with `remove-live-install.mjs`, once per target (overlay, then global),
using the 5.1.35 baseline backups:

```powershell
node "<attempt>/live-install/remove-live-install.mjs" --target "C:/Users/USER/.omo-stable-500/staging/memory-rollback-5.1.35-r1/package/plugin/extensions/omo-task.js" --backup "C:/Users/USER/.omo-stable-500/home/.omo/agent/.agent-coordination-install/adapter/omo-task.js.5135-baseline-overlay"
node "<attempt>/live-install/remove-live-install.mjs" --target "C:/Users/USER/.bun/install/global/node_modules/omo-ai/plugin/extensions/omo-task.js" --backup "C:/Users/USER/.omo-stable-500/home/.omo/agent/.agent-coordination-install/adapter/omo-task.js.5135-baseline-global"
```

Add `--dry-run` first to check without writing. Re-apply with
`apply-live-install.mjs` as shown above, using a fresh `--backup` path that does
not exist yet: reusing the removal backup is refused with
`BACKUP_ALREADY_PRESENT_REFUSED` for both targets.

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
