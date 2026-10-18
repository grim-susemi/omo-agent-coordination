# Disposable candidate installation

Run from the product root with Bun. This candidate installer requires an explicit
agent directory strictly below the fixed attempt directory:

`.omo/evidence/agent-coordination/attempt-20261017-e1/`

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
`75e2e91b945ebc19559aa7935383a1239ce4b7b5cf28589485145b564b883198`.
The builder also verifies pinned AgentSession SDK bytes and unique structural
anchors. Its manifest records each exact before/after anchor, bootstrap,
baseline/candidate hashes and why the native owner-engine connection is required.
The disposable native fixture must load this adapted target; merely installing
the collaboration entry cannot grant owner-engine authority.

The delta connects only the task component engine, original outcome ports,
actual task SDK readiness and exact runner-owned runtime. It is inert without
the subscribing mode. It is not a second scheduler or a vendored engine.
Source version: omo-ai 5.1.34, upstream commit
`d7e224e478a4a77dba0d2104c336fdc72ad301e3`. See scripts/build-adapter.mjs and
the generated manifest for the complete delta.

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
