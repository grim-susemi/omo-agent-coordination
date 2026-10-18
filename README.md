# Agent coordination mod (verification candidate)

Owned root sessions and their permitted descendants can inspect work, ask a
peer using that peer's live context, or leave a note for its next natural turn.
This does not replace native task, task_send, task_output, or task_cancel.
No live profile has been installed by this project.

## Tools and working habits

- `coordination_inspect({target_ref?})` reads one issued address or the owned
  tree. Recorded state, runtime activity, outcome, publication, notification,
  capacity, residency and coverage are separate facts. Unknown is not dead.
- `coordination_ask({target_ref,question,request_id?,depth?,timeout_ms?})` makes
  one extra, tool-free request to the target's actual model. Quick is the
  default: request-local low-cap-v1, 4,096 total output tokens including
  reasoning, 2,048 displayed Unicode characters, 30 seconds by default and
  60 seconds maximum. A low setting is never raised. Unsupported controls
  refuse before calling the provider.
- Explicit `depth:"deep"` uses the target's effective thinking and output
  limits unchanged, with no mod deadline or display truncation. It blocks the
  asker until a reply or lifecycle/cancellation failure and costs the target
  model's full request price. Deep plus timeout_ms is invalid. Cancelling the
  asker cancels only the side request; late output is discarded.
- `coordination_notify({target_ref,message,request_id?})` costs zero immediate
  model calls. It queues a note without starting or interrupting a turn.

Use inspect for status, native task_send for instructions, notify for one-way
information, and ask for unexpected state, shared-resource conflicts or peer
judgment that tools cannot answer. Include a short artifact reference; aim for
one round trip. Do not resend repeatedly when an answer is missing. A new
question based on new evidence needs a new request ID.

Readonly explore, librarian and oracle workers may collaborate without gaining
file-edit or shell permissions. One-shot reviewers, independently assigned
reviews and explicitly forbidden assignments remain excluded. Issued target_ref
values are addresses, not authorization. A side reply is peer information, not
user approval, a new assignment or completion evidence.

Accepted exchanges are queued with native nextTurn delivery and triggerTurn=false.
Queued is not read: incorporated requires observed native consumption. If no
natural turn occurs, or the target/epoch/generation ends, the exchange is dropped.
Reload removes only this mod's unconsumed messages; unrelated queued input remains.
Targets born before capture are unavailable (`capture_missing`); they are not
restarted. A repeated request ID and identical content shares the original
result; changed content, including depth, conflicts. There is no paid replay,
automatic heartbeat, model fallback or mod-level retry.

## API v1 for other mods

Import `getCoordinationBroker` or `getCoordinationClient` from
`piomo-agent-coordination-mod/api`. Importing this helper has no capture, SDK,
provider or extension-entry side effects. Pass the actual current native
ExtensionContext to get a scoped client; copied session/role strings are not
authority. Discovery uses `Symbol.for("omo.agent-coordination.api.v1")`.

The broker has only `version:1`, frozen `features` and `getClient(ctx)`.
The client exposes `peers()`, `ask(input)`, `notify(input)`, `inspect(target_ref)`,
`setAskPolicy(input)`, `onPeersChanged(listener)` and `dispose()`.
Programmatic ask/notify require request_id. Tool and API requests share
authorization, duplicate handling, concurrency and pending-message limits.
Subscriptions fire only on roster revision changes; dispose removes this
client's listeners and policies, not another client's runtime.

`inspect` returns live, status, run_epoch, last_transition_at, record_consistent
and asks without calling a model or changing state. The owning root alone can
set a worker target's `deny_peer_asks` policy and optional allow_from refs.
Unlisted callers, including root, cannot ask or notify it; its own outbound
questions remain permitted. Native exclusions still win. Policy changes cancel
disallowed in-flight side requests and drop only their unconsumed mod messages.
Policies expire on client disposal, target settlement, epoch or generation change.

Check features before using optional ports. Only wired, validated functionality
is advertised: `ask_depth_deep`, `ask_policy`, `inspect`, and
`revival_settlement_repair`. Features are frozen for a generation. Missing mode,
wrong API version and old clients return typed refusals, never a hidden runtime
or provider fallback.

## Lifecycle and support

`OMO_AGENT_COORDINATION=0` disables capture and coordination tools. Reload to
apply a switch. `OMO_AGENT_COORDINATION_REPAIR=0` disables automatic terminal
repair and omits revival_settlement_repair; read-only mismatch inspection remains.
Only the same native owner/epoch/handle's proven outcome may authorize repair
through the original engine transition, publication, notification and release
path. Age, model text and silence do not authorize mutation.

| Component | Supported candidate |
| --- | --- |
| OMO | omo-ai 5.1.34, pinned native bundle SHA-256 in adapter manifest |
| Senpi | 2026.10.16, actual loader/task namespace identity and SDK hashes checked |
| Host | Windows, Node 24 / Bun; native process transport is an owned named pipe |
| Other versions or shapes | Explicit unsupported; original native work remains unchanged |

The package uses pi.extensions with one directly loaded ESM `dist/extension.js`;
Senpi is external, not vendored. See INSTALL.md for disposable install, removal,
backup/hash checks and drift refusal. Final integration and independent review
are separate gates; this is not a claim of final product acceptance.

## 한국어 요약

같은 소유 작업 트리에서 상태 조회, 동료의 실제 문맥을 이용한 1회 질문,
다음 자연 차례에 읽을 쪽지를 제공합니다. quick이 기본이며 deep은 매번
명시해야 하고 대상 모델의 원래 한도와 비용을 사용합니다. 답변은 사용자
승인이나 작업 완료 근거가 아닙니다. 실사용 프로필에는 설치하지 않았으며,
설치·제거는 백업과 해시가 맞는 격리 시험 디렉터리에서만 수행합니다.
