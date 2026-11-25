# Settlement and execution

## Implementation status: Neo X and Neo N3 decoder batches

The datastore now has additive `lifecycle:v1:*` records keyed by bridge type,
direction, nonce, and (for tokens) both token contracts. They preserve distinct
positive observations for settlement, distribution, claimability, claim,
execution, and message type, each with its own chain/transaction/block position,
contract, implementation and optional original result payload. A repeated
observation is idempotent; contradictory evidence is rejected. Observations
can arrive before the source operation. No legacy `op:*`, status index,
completion timestamp, or destination transaction is migrated or rewritten.
Missing sidecars and absent individual facts mean **unknown**.

Separate `lifecycle_recovery:v1:*` manifests store each chain's watched
contracts, implementation ranges, evidence paths, inclusive replay bounds in
that chain's own heights, every target and its hash, and the contiguous
watermark/hash. The `recover` command replays ranges through the live decoders
and commits each range with its watermark in one transaction; see
[Recovery command and runbook](#recovery-command-and-runbook). A contiguous
manifest alone does not certify claim coverage or enable any negative
conclusion: unresolved claim records and Neo X claim trace gaps inside the
bounds keep it incomplete.

The Neo X live indexer writes decoded destination events, Neo X message
source types and verified direct and indirect silent token claims with its
normal checkpoint. Blocks, receipts and watched events use standard RPC and
are required: a failed fetch, malformed watched event, conflicting evidence or
failed write prevents checkpoint advancement. `debug_traceBlockByNumber` call
trees are optional enrichment, needed only for indirect claims (see
[Neo X claim trace gaps](#neo-x-claim-trace-gaps)). Each successful indirect
claim requires successful ancestors and a successful outer receipt. Only the
verified historical silent-claim implementation is enabled;
another implementation stops processing a correlated claim until verified.
Claims without an observed earlier claimable retain unknown outcome and require
historical recovery. The sole checked-in message bridge release artifact (v1.0.0)
declares `Execution(uint256,(bool,bytes))`. No verified deployed ABI for the
old indexer signature `Execution(uint256,uint8)` has been identified, so an
observed event with that signature stops the checkpoint instead of interpreting
it as the tuple. These limits mean the live checkpoint does **not** certify
complete historical coverage. The N3 live indexer now records received roots,
distribution, claimability, emitted claims, and typed or chunked message
execution atomically with its checkpoint and legacy updates. It fetches
application logs for every transaction, including indirect calls. Missing logs
and malformed watched notifications stop the checkpoint. A committed token claim
without a verified claimable pair is retained as an unresolved notification
alongside the checkpoint; its lifecycle claim stays unknown until recovery.
A received root replaces same-nonce synthetic
destination state. No public API fields or alert and webhook rules change.
Historical recovery tooling exists; a verified production recovery and cutover
remain prerequisites to the public projection and relay-stuck precedence.

This document records the lifecycle contract adopted for backend issues [#28](https://github.com/bane-labs/bridge-indexer-backend/issues/28) and [#29](https://github.com/bane-labs/bridge-indexer-backend/issues/29), and frontend issues [#9](https://github.com/bane-labs/bridge-indexer-frontend/issues/9) and [#17](https://github.com/bane-labs/bridge-indexer-frontend/issues/17). It specifies the implementation to follow. The current operation API still exposes the legacy completion model.

## Contents

- [Lifecycle rules](#lifecycle-rules)
- [Event mapping](#event-mapping)
- [API and compatibility](#api-and-compatibility)
- [Frontend presentation](#frontend-presentation)
- [Recovery and verification](#recovery-and-verification)
- [Contract evidence](#contract-evidence)

## Lifecycle rules

Settlement records arrival on the destination chain. Execution records the applicable destination action. Preserve each observation's chain, block height, transaction hash, event position, and block timestamp independently.

| Operation | After arrival | Final destination observation |
| --- | --- | --- |
| Native/token distributed during relay | Settled and executed | Distribution in the relay transaction |
| Native/token made claimable | Settled and not executed | A later successful claim |
| Executable message | Settled and not executed | Committed message execution |
| Store-only or result message | Settled, execution inapplicable | Arrival itself satisfies the operation's delivery requirement |

An execution event records that the message bridge executed the message. It does not promise that every target call succeeded. Preserve the contract's result payload. A committed execution whose result reports failure is still executed. A reverted transaction or N3 FAULT execution establishes no lifecycle transition. This follows the contracts' executable-state behavior, rather than interpreting the result as an arrival signal.

A source event alone establishes a request. This applies equally to executable, store-only, and result messages. The synthetic destination records used by the legacy implementation are bookkeeping and cannot establish arrival in the new lifecycle.

Directional sync compares requested nonce/root with received nonce/root for the same directional instance. Native and message instances are chain-direction-specific. Token instances also include both token contracts. An execution or claim must not replace the received root or the original settlement transaction/time. Compare roots by bytes, accepting their existing hexadecimal prefix and casing representations.

Apply lifecycle facts monotonically. A duplicate or older arrival must not reverse execution, and a later claim must not replace settlement provenance. Source and destination indexers may observe operations in either order. Retain destination observations until their source operation is available. Distinguish operations with equal nonces on different token pairs.

The monitoring threshold applies to missing arrival. Settlement observations govern the relay-stuck detector even while legacy fields retain their old projections. Apply these rules in order.

1. Confirmed settlement suppresses relay-stuck classification and alerts, including reminders, even when legacy `status` is `pending` or `stuck`. Remove any corresponding internal relay-overdue marker. Preserve the compatibility status independently. An operation awaiting a user claim or message execution after arrival cannot become relay-stuck.
2. Without confirmed settlement and with incomplete recovery or live observation coverage, treat relay-overdue assessment as unavailable. Do not mark the operation newly stuck or send/repeat an operation-stuck alert from its legacy status. Report incomplete coverage through recovery progress and logs. Resume assessment only after the applicable destination interval is verified complete.
3. With complete destination coverage and no settlement, apply the existing per-bridge threshold to the source timestamp. Before sending, recheck settlement and coverage atomically with the overdue transition so concurrent arrival cannot generate a stale alert.

Keep the current message-withdrawal alert exemption during this rollout. Clearing an overdue marker does not introduce a new resolved-webhook notification in this step. General liveness alerting remains covered by backend #15 and its follow-up. Test confirmed settlement with each legacy status and incomplete coverage both before and after a previously sent alert.

## Event mapping

The mappings below come from the official contract interfaces and implementations linked under [Contract evidence](#contract-evidence). An event declaration alone does not establish that an implementation emits it. Associate observations with the deployed implementation at their transaction position, chain, direction, and token pair. Events in committed transactions and verified successful claim calls establish observations.

### Neo N3 to Neo X

| Chain and event | Fact |
| --- | --- |
| N3 `NativeDeposit` / `TokenDeposit` | Source request, source nonce/root and token-pair identity |
| Neo X `NativeDepositRootUpdate` / `TokenDepositRootUpdate` | Destination received nonce/root and relay transaction/time |
| Neo X `NativeDeposit` / `TokenDeposit` | Distribution for the operation in the relay transaction, execution recorded |
| Neo X `NativeClaimable` / `TokenClaimable` | Arrival with funds available to claim, execution outstanding |
| Neo X `NativeClaim` | Execution through claiming, original settlement retained |
| Neo X verified successful `claimToken(address,uint256)` call | Token execution through claiming for implementations that omit `TokenClaim`, original settlement retained. See the claim evidence rules below. |
| N3 `MessageSend` | Source message request, type, nonce/root and metadata |
| Neo X `NeoToEvmRootUpdate` | Destination received message nonce/root and relay transaction/time |
| Neo X `Store` | Per-message arrival and metadata |
| Neo X `Execution` | Execution of the stored executable message and its result |

### Neo X to Neo N3

| Chain and event | Fact |
| --- | --- |
| Neo X `NativeWithdrawal` / `TokenWithdrawal` | Source request, source nonce/root and token-pair identity |
| N3 `NativeWithdrawalRootUpdate` / `TokenWithdrawalRootUpdate` | Destination received nonce/root and relay transaction/time |
| N3 `NativeWithdrawal` / `TokenWithdrawal` | Distribution for the operation in the relay transaction, execution recorded |
| N3 `NativeClaimable` / `TokenClaimable` | Arrival with funds available to claim, execution outstanding |
| N3 `NativeClaim` / `TokenClaim` | Execution through claiming, original settlement retained |
| Neo X `MessageSend` | Source message request, type, nonce/root and metadata |
| N3 `EvmToNeoRootUpdate` | Destination received message nonce/root and relay transaction/time |
| N3 `Store` | Per-message arrival and metadata |
| N3 `Execution` | Execution of the stored executable message and its result, potentially chunked |

Root updates describe batch receipt. Preserve the exact checkpoint root from that event. Correlate the accompanying per-operation distribution, claimable, and storage events by nonce and token identity. A later claim provides execution provenance, rather than a substitute relay timestamp. All events in one transaction must be combined before publishing its lifecycle state.

The contracts require consecutive incoming nonces when accepting a batch. A received checkpoint can establish coverage for the corresponding source requests, while each operation's outcome comes from its own events. Recovery must find the original covering relay transaction instead of assigning a later checkpoint's transaction to every historical operation.

### Deployed bridge versions

The decoders follow the implementation active at each event's height. Activation heights were established from chain state: the N3 ContractManagement update counter and NEF checksum at historical state roots, and the Neo X EIP-1967 implementation slot cross-checked against the proxies' `Upgraded` logs. All transactions succeeded.

| Chain and contract | Version | Active from | Transaction | Identity |
| --- | --- | --- | --- | --- |
| N3 Bridge `0xbb19cfc8…13d2` | 1.0.0 (probable) | 5,751,638 (2024-07-25) | `0x36f6e022…cb03` | ABI matches tag 1.0.0; no artifact, byte identity unproven |
| | 2.0.0 | 6,304,759 (2024-11-06) | `0xb5956dc2…3b27` | NEF byte-identical to `release-artifacts/token-bridge/2.0.0` |
| | 3.0.0 | 6,877,325 (2025-02-24) | `0xc36c411e…c788` | NEF byte-identical to the 3.0.0 release |
| N3 MessageBridge `0xde9cc59d…bd11` | message-bridge/1.0.0 | 8,328,582 (2025-11-12) | `0xb3b44355…6309` | NEF byte-identical to the release |
| Neo X Bridge `0x1212…0004` | 1.0.0 | genesis | — | byte-identical to the 1.0.0 artifact |
| | 2.0.0 | 655,999 (2024-11-06) | `0xd39df0ed…4d3e` | executable code identical (own-address immutable and metadata hash differ) |
| | 3.0.0 | 1,772,382 (2025-02-24) | `0x6b02b68e…cb08` | identical apart from the own-address immutable |
| Neo X MessageBridge `0x1212…0009` | placeholder | genesis | — | upgrade-only stub without events; no tagged ABI applies |
| | message-bridge/1.0.0 | 4,721,793 (2025-11-13) | `0xca1d72c9…a349` | executable code identical |

Before 3.0.0 both chains named the native (GAS) bridge events `Gas*`: `GasDeposit`, `GasDepositRootUpdate` (Neo X), `GasClaimable`, `GasClaim`, `GasWithdrawal`, `GasWithdrawalRootUpdate` (N3), and `GasBridgePause`/`GasBridgeUnpause`. 3.0.0 renamed them `Native*`. Fields and their order, the 8-decimal event amounts and the root events are unchanged, the nonce sequences continue (Neo X deposits 1–1316 then 1317, withdrawals 1–440 then 441), and both families decode to the native bridge type. On-chain emission was verified for every `Gas*` operation event except `GasClaim`, which was never emitted on Neo X. On mainnet, for the contracts above, a `Gas*` event at or after the 3.0.0 height, a `Native*` event before it, an N3 bridge notification before its deployment, or a Neo X MessageBridge log while the placeholder was active stops the range. The network is identified by Neo X `eth_chainId` (mainnet 47763) and the N3 network magic (mainnet 860833102). Neo X testnet uses the same native contract addresses with a different upgrade history (for example, a `NativeDepositRootUpdate` at testnet block 1,732,185, below mainnet's 3.0.0 height), so on any other network, and for any other contract address, both families are accepted without height rules. Token events are unchanged across versions. `claimToken` emits no bridge event in any version, while native claims emit `GasClaim`/`NativeClaim`.

### Decoder details

- Preserve contract-version event signatures during historical replay. The Neo X interface at the pinned revision declares `Execution(uint256,(bool,bytes))`. The current indexer listens for `Execution(uint256,uint8)`. Resolve the ABI applicable to each deployment/version and support historical versions established by chain evidence. Do not treat these signatures as interchangeable.
- Decode N3 executable metadata using its serialized representation and Neo X metadata using its ABI representation. Map `EXECUTABLE` / `STORE_ONLY` / `RESULT` to public `execute` / `store` / `return`.
- N3 `Execution` has fields `(Nonce, NumberOfChunks, Index, Result)`. When `NumberOfChunks = 1`, require `Index = 0` and preserve `Result` as its original typed VM stack item, including arrays, integers, booleans, nulls, and byte strings. Do not require bytes or deserialize that item again. A one-event byte-string result is also a typed result, rather than a serialized multi-chunk payload.
- When N3 `NumberOfChunks > 1`, each result is a byte-string slice of a serialized VM result. Group by chain, emitting contract, transaction, and nonce. Deduplicate identical replayed event identities first. Require a consistent positive count and exactly one slice for every index `0..count-1`, concatenate in index order, and deserialize once using N3 VM serialization. Keep the original slices as provenance and create one execution observation. Missing, conflicting duplicate, inconsistent, or out-of-range chunks prevent checkpoint advancement for the affected work. Tests must exercise typed single-event results and multi-event serialized results through the real notification decoder, plus malformed chunk groups.
- A recognized event with invalid fields, an unavailable required application log, or a failed persistence operation must prevent checkpoint advancement for that work unit. Unrelated events can be ignored. A transaction-script prefilter must not discard indirect calls that emit watched events.

### Neo X token claim evidence

The pinned `BridgeImpl.claimToken` checks that a claimable record exists, deletes it, and calls `SafeERC20.safeTransfer` to its stored recipient. It emits no `TokenClaim`. A successful claim must therefore be detectable even if the bridge emits no log in that transaction.

For this implementation, require a successful receipt and an ABI-decoded call to the bridge's `claimToken(address,uint256)`, using the implementation active at that call. A direct top-level call is established by the transaction's destination and input. An indirect call requires a successful call trace identifying the bridge call and its token/nonce arguments, with successful ancestors and outer receipt. Match the instance and token/nonce to the prior claimable observation. The caller need not be the recipient. The pinned function's successful completion proves consumption of that claimable and a non-reverting safe transfer. Record the containing transaction as `execution_tx_hash`, with the call position as provenance.

Where the token emits a matching ERC-20 Transfer, retain its bridge sender, stored recipient, amount, and log position as corroborating evidence. A Transfer by itself cannot identify a claim nonce. A missing latest claimable record cannot identify the historical claim transaction. Neither is sufficient alone. For a different deployment version, use its verified claim implementation and emitted events. Accept `TokenClaim` only for a version known to emit it, and deduplicate event and call evidence from the same execution.

Live processing and recovery must inspect successful transactions/calls as well as bridge-emitted logs. Cover indirect claims through traces or equivalent verified historical execution evidence. Unsupported implementation versions, missing required traces, and uncorrelated claims remain unresolved and block execution-coverage completion. They must not be classified `not_executed` merely because no `TokenClaim` was found.

### Neo X claim trace gaps

Normal indexing must work with standard RPC. A direct top-level `claimToken` call is fully established by the block's transaction, its successful receipt and the historical implementation lookup, so it is recorded without a trace. An indirect call is visible only in a call trace. When the active endpoint cannot trace a block, the indexer still commits that block's events, receipts and direct claims, and records the block in a `lifecycle_unresolved:v1:neo_x:claim_trace:<from>` range in the same transaction as the checkpoint. The range's reason is `trace_unsupported` (JSON-RPC -32601), `trace_unavailable` (request failed), or `trace_invalid` (trace inconsistent with the block). Blocks without transactions need no trace.

- A gap means indirect claim outcomes in those blocks are unknown. Transaction success alone never establishes a claim, and a missing claim in a gap is never `not_executed`. `Store.NeoXClaimCoverageGaps(from, to)` must be empty, in addition to verified recovery coverage, before any negative claim conclusion for that interval.
- The Neo X client sends one trace request per attempt, without retry or failover. A -32601 answer is cached for the responding endpoint URL only and re-probed every 10 minutes; another endpoint reached by failover is still traced. After one failed trace request, the rest of that range is recorded as a gap instead of retried.
- While the live indexer is idle, it traces the lowest gap block, re-derives every claim in it with the same decoder, and atomically writes those observations and removes the block from the gap (`ResolveNeoXClaimTraceGap`). Failures keep the gap and back off for a minute. Enrichment never moves or resets the live checkpoint, and reapplying an already stored claim is idempotent. Gaps can be drained later by adding a tracing endpoint to `NEOX_RPC_ENDPOINTS`; no checkpoint reset or database wipe is needed.
- Historical recovery on Neo X is event-only by default and records each range as a `claims_deferred` gap; claim reconciliation (below) resolves them. Live trace enrichment skips `claims_deferred` gaps.

A future enrichment source, such as a node-specific event plugin that reports `claimToken` calls, fits at the same point as the trace. It must report every successful bridge `claimToken` call in a block, with its call position and successful ancestors, so that it proves complete claim coverage for that block. It then feeds the same claim decoder and resolves the same gap records. A source that can only confirm some calls may add positive claim observations but cannot remove a gap. No source may become a prerequisite for advancing the normal checkpoint.

The captured [successful token claim fixture](fixtures/neox-token-claim-without-event.json) contains the raw claim transaction, successful receipt, preceding root-update/claimable receipt, and historical EIP-1967 implementation lookup. It covers Neo X token `0xc28736dc83f4fd43d6fb832fd93c3ee7bb26828f`, nonce 108. The claim transaction `0xeba5f8409858a106934f567f9562e5ab9269835d938a6f64d0031a3ccfb04ab4` emits one token Transfer and zero bridge logs. Its sender differs from the claimable recipient. Tests must mark it executed, preserve settlement transaction `0xf81aeb05ae05142e69eb61414cfdfcc8b947ac0019357ce9dddb3d1fb64c29f3`, and link the claim transaction separately. Include negative variants for a reverted receipt, unrelated Transfer, mismatched token/nonce, and a failed ancestor call in an indirect claim trace. The implementation resolved for the fixture has verified source with the same silent `claimToken` behavior. [Historical implementation](https://xexplorer.neo.org/address/0x671bcb1b24e75a37adb283ffa509620c02eb35c6?tab=contract)

## API and compatibility

Add these operation fields to the existing list and single-nonce responses. The mixed casing below intentionally follows backend #29.

| Field | Value |
| --- | --- |
| `settlementStatus` | `settled` or `not_settled` |
| `settledAt` | Block timestamp of original destination arrival, or `null` |
| `settlement_tx_hash` | Original destination arrival transaction, or `null` |
| `executionStatus` | `executed`, `not_executed`, or `null` for store/result messages |
| `execution_tx_hash` | Distribution, execution, or claim transaction, or `null` |
| `message.type` | `execute`, `store`, or `return`, present for messages |

Native operations use the same execution fields as token operations. A new, unrelayed asset operation or executable message has `settlementStatus = not_settled`, `executionStatus = not_executed`, and null destination provenance. Store/result messages have null execution status and hash before and after arrival. Immediate distribution can have identical settlement and execution transaction hashes.

Unknown message types or incomplete historical coverage must not be guessed. Publish the new lifecycle projection only when the record has sufficient decoded provenance and recovery coverage through its current destination processing watermark to establish it. Otherwise omit the whole projection and retain the legacy fields. A missing event during an incomplete historical scan is not evidence that execution did not happen. After verified recovery and continuous live coverage, absence of execution evidence establishes `not_executed` as of that watermark. Token execution coverage includes silent successful claim calls, as specified above.

Retain source fields, token identities, amounts, pagination, `status`, `completion_timestamp`, `dest_tx_hash`, and `dest_block_height` during rollout. Add optional `settlementStatus` and `executionStatus` query filters and retain the existing `status` filter.

The legacy fields retain their existing projection for older consumers, including the current pending/completed/stuck transitions and completion transaction behavior. New root-update observations alone must not silently change the legacy meaning of completed. A later claim may continue to update the legacy completion transaction while the new settlement transaction remains the original relay transaction. Legacy synthetic completion stays confined to the compatibility projection.

Execution applicability and event provenance belong to the new model. Never derive its values solely from legacy `status`, destination hash presence, or synthetic destination state. Preserve original records during migration and verify the legacy projection with regression tests.

Update backend Swagger and the frontend OpenAPI contract together when these fields are implemented. Frontend wire types must be generated from that contract. A later breaking-change PR removes deprecated fields, filters, and frontend fallbacks after production verification and a consumer audit. The additive release can close backend #29 with that cleanup tracked separately.

## Frontend presentation

Use separate relay progress and outcome columns, resolving the overlap between frontend #9's removal of the combined state and frontend #17's Relayed/Completed wording.

| Observed lifecycle | Relay progress | Destination outcome | Transactions |
| --- | --- | --- | --- |
| Awaiting arrival | Awaiting relay | Unavailable | Source transaction |
| Distributed during arrival | Relayed | Completed | Relay transaction and distribution transaction, possibly identical |
| Arrived and claimable | Relayed | Claimable | Relay transaction |
| Claimed after arrival | Relayed | Completed | Original relay transaction plus claim transaction |
| Executable message arrived | Relayed | Awaiting execution | Relay transaction |
| Executable message executed | Relayed | Executed | Original relay transaction plus execution transaction |
| Store-only message arrived | Relayed | — | Relay transaction, message type Store |
| Result message arrived | Relayed | — | Relay transaction, message type Result |

Show the original relay transaction as **Relayed Tx** and its timestamp as **Settled**. Keep the execution/claim transaction separately accessible with the destination-chain explorer link. Show executable message type as **Execute**. Render unavailable values explicitly rather than literal null values. Executed describes the protocol's recorded execution, with the result retained as separate evidence.

When the complete new projection is absent, retain the current legacy presentation. Do not combine one new lifecycle field with guessed legacy values for the remaining fields. The fallback supports the older backend during rollout and is removed in the later cleanup.

Directional cards use received nonce/root, independently of the history row's outcome. A synced direction can therefore include Claimable and Awaiting execution rows. Freshness needs evidence independent of elapsed time since the last bridge operation. Continue displaying unknown freshness until that evidence is available.

## Recovery and verification

Deploy additively on `develop` and validate with a database copy. Replay destination lifecycle events and root updates for existing indexed operations using the same decoders and transitions as live indexing. Store replay progress independently of normal indexing checkpoints. Include completed, pending, and stuck operations, and preserve existing source history.

### Coverage bounds and cutover

Maintain a recovery manifest separately for Neo N3 and Neo X. Record chain identity, watched contracts and implementation-version ranges, the inclusive start and target heights, target block hash, and last contiguous committed recovery height/hash. A saved completion flag without those bounds is insufficient.

- Set the start height to the first deployed block of each applicable destination contract, using verified deployment history. If configuration starts later, explicitly validate that the excluded interval contains no relevant operation or outcome. Use the earliest required destination deployment across the records being recovered. Never use a Neo N3 source height as a Neo X replay height or the reverse. Source timestamps may aid validation but cannot replace destination bounds.
- For the database-copy rehearsal, capture a target height/hash on each destination chain and replay its full required interval through that inclusive target. Coverage requires all relevant logs, notifications, successful claim calls, and required traces to have been processed and persisted. Skipped fetches, decoder errors, unsupported versions, or unresolved claim provenance keep the interval incomplete. Commit recovery observations and their contiguous watermark together.
- The rehearsal proves the migration procedure, not production completion. For production cutover, pause the indexers and the stuck detector, back up the database, and capture the persisted normal indexing height for each chain. Choose a destination cutover target at least as high as both that chain's persisted height and its current eligible chain tip. Use the chain's existing confirmation policy. Recover the production database through those targets, including every block added after the copy. Verify target hashes, complete manifests, and incident checks before restarting.
- Resume ordinary processing at the first block after each chain's verified cutover target, with observations, coverage marker, and normal checkpoint made consistent before enabling the new projection. Retain rollback backups. If recovery stops early, do not resume a checkpoint ahead of the recovered watermark or enable the new lifecycle projection. Re-run recovery from its committed watermark.
- After restart, live observations and their coverage/checkpoint advancement remain atomic. Events in blocks beyond the captured targets are processed normally. A gap or target-hash mismatch invalidates the affected coverage and requires replay before negative execution conclusions or relay-stuck assessment can resume.

Recovery is complete only when every required destination interval is contiguous through its captured cutover target, every implementation version and required evidence path is supported, no required operation remains unresolved, and normal indexing resumes contiguously. Each subsequent complete live watermark extends that coverage. Recovery progress output must expose chain, start, target, contiguous height, and unresolved count.

Reconcile durable `lifecycle_unresolved:v1:neo_n3:token_claim:*` notifications against historical claimable evidence before claiming N3 execution coverage. A live checkpoint can advance past these notifications, but that checkpoint alone cannot certify a negative execution outcome for the affected interval.

Recovery tests must include a destination outcome added after the rehearsal snapshot but before production cutover, an interrupted replay with a normal checkpoint ahead of its recovery watermark, and a failed trace or block fetch inside an otherwise complete range. Each incomplete case must keep the new negative projection and operation-stuck alerts unavailable. Only a verified contiguous replay and handoff may enable them. Exercise both chains with different numeric height ranges.

Retries must be idempotent. Rebuild directional state from real source and received observations. A copy-and-replay comparison must produce the same lifecycle as live processing, including when destination events precede source indexing. Report unresolved records and incomplete ranges. They block production promotion.

The known incident checks from the November 20, 2026 investigation are acceptance examples, rather than fixed live values.

- Message Neo X to N3 had source nonce 15 and indexed destination nonce 13. N3 `evmToNeoNonce` returned 15 and `evmToNeoRoot` matched the source root `bd8a3be21d32d769c8f80e7f803166aa7c29bdfb72a315aabb3678869eb324f6`. Recover the covering receipt and per-message storage independently of execution.
- NDMEME deposit nonce 27 had N3 source height `13051516` and a Neo X `TokenDeposit` observation in transaction `0xc34ba5e2efcc32a5c6b317c87e8ad7b4e5e2113e9a0eb333e6f79cc8a4f6cdb2`, log index 2, block `0x74acee`. Topic zero was `0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b`, token `0xe816dee05cf6d0f2a57eb4c489241d8326b5d106`, nonce 27. The recovery regression must find this lower-numbered destination block using Neo X bounds. Starting a Neo X scan at N3 height `13051516` must fail the test. Recover arrival and distribution and verify the history and direction. [Frontend #22](https://github.com/bane-labs/bridge-indexer-frontend/issues/22)
- NEO withdrawal nonce 11 was reported distributed in N3 transaction `0x1c4c374824b8059230e3bd1dc8c780294531bed4c591de72032119372a79c1f8`. Its actual application log (N3 block `6818342`, HALT) holds `TokenWithdrawalRootUpdate` (nonce 11, Neo X token `c28736dc83f4fd43d6fb832fd93c3ee7bb26828f`, NEO `ef4073a0f2b305a38ec4050e4d3d28bc40ea63f5`) at `0:0` and `TokenWithdrawal` (amount 1) at `0:4`. [Frontend #16](https://github.com/bane-labs/bridge-indexer-frontend/issues/16)

These are captured from mainnet in [recovery-incidents.json](fixtures/recovery-incidents.json) by `go test -tags capture -run TestCaptureRecoveryFixtures ./internal/indexer`, which runs the recovery code against public nodes through a recording proxy; the tests replay the recorded responses through the same code. The message relays are N3 blocks `14720004` (`EvmToNeoRootUpdate` 14 and `Store` 14) and `14720342` (`EvmToNeoRootUpdate` 15 with root `bd8a3be2…24f6`, and `Store` 15), with no `Execution` in either transaction. The NDMEME source is N3 block `13051516`, transaction `0x7f3a35997b347304c5f3de909e20c4c8ddc4fb17b488646ab36dbf820f2cc4a0`, whose deposit root equals the Neo X received root `5c744169…d6bc83`. These fixtures verify the recovery code against real evidence. They do not show that the production database has been recovered.

The persistence and decoder PRs must cover immediate distribution, claimable then claimed, stored and executable messages, committed execution results, reverted attempts, chunked events, multiple token pairs at equal nonces, retries, and out-of-order processing. Preserve the legacy response and test the new frontend through its API request path with reusable fixtures, MSW, and providers.

Before production, back up the database and rehearse migration and rollback on a copy. Recover and verify the backend first, verify both API projections, then deploy the frontend. Retain the original database until verification completes. Production deployment requires separate authorization. Replaying a source chain from its bridge deployment also inserts source operations missing from the database (frontend #19).

### Recovery command and runbook

Historical recovery supports Neo X and N3 **mainnet only**: `begin`, `run` and `claims` refuse any other network, because the version table, the claimable storage slot and the verified claim implementations are mainnet facts. Live indexing on other networks is unaffected.

The network is a property of the whole endpoint list, not of one connection. The Neo X client asks every configured endpoint for `eth_chainId` at startup and refuses a list whose reachable endpoints disagree. It pins that chain ID, and a failover never switches to an endpoint that reports a different one or cannot report one. An endpoint unreachable at startup is checked when failover first reaches it. The N3 client checks the network magic of every pooled connection at startup; the pool never adds connections later. This applies to the server too: a mixed-network endpoint list stops it at startup instead of mixing two chains' data.

`bridge-indexer recover` uses the server's configuration (`DB_PATH`, `NEO_N3_RPC_ENDPOINTS`, `NEOX_RPC_ENDPOINTS`, `N3_NEOX_BRIDGE`, `N3_MESSAGE_BRIDGE`, `NEOX_TOKEN_BRIDGE`, `NEOX_MESSAGE_BRIDGE`, `N3_SYNC_CONCURRENCY`, the RPC call intervals). Each chain has its own manifest and is replayed separately; one replay covers both roles of that chain, so the N3 replay recovers N3 source deposits and N3 destination evidence for withdrawals, and the Neo X replay the reverse.

| Command | Effect |
| --- | --- |
| `recover begin --chain C --start S --target T` | Creates the manifest for `[S, T]` in chain C's heights. `T` is a height, `head` or `checkpoint` (the live checkpoint). The target must exist on C now and its hash is captured from C. Rerunning with the same bounds resumes; different bounds are refused. |
| `recover run --chain C [--batch N] [--max-blocks M] [--concurrency K] [--claim-scan]` | Replays from the watermark. Each range of at most N blocks is committed atomically; `--max-blocks` bounds one invocation. On Neo X the default is event-only: one bounded `eth_getLogs` per range over the Bridge and MessageBridge addresses (N defaults to 100,000; an `eth_getLogs` failure halves the range down to 1,000 without committing, and successes grow it back), plus a receipt for every transaction with a bridge log and a header for every block with one. No other block is fetched, and the whole range is recorded as `claims_deferred`. `--claim-scan` restores the per-block silent-claim scan of live indexing (N defaults to 250; every block is fetched, and traced when the endpoint can). K bounds parallel header and block fetches (default 1); the public endpoint answers HTTP 429 even at 3, so raise K only on a dedicated node. N3 defaults to 100. Ctrl-C or an error keeps the last committed range. Rerun to resume. Before starting it re-checks the target hash against C and the configured contracts against the manifest. |
| `recover extend --chain C --target T` | Raises the target, keeping progress. A target can never be lowered or re-hashed. |
| `recover claims --chain neo_x` | Reconciles silent token claims through the committed height H from contract state (see below). Safe to rerun; the result replaces the previous reconciliation. |
| `recover status [--chain C] [--json]` | Opens the database read-only (only while neither the server nor `recover run` has it open) and prints start, target, committed height, live checkpoint, counts, gap blocks, unresolved claims, `contiguous`, `complete`, `handoff ready`, and the blocking reasons. |

Each committed range writes, in one transaction: lifecycle observations (a conflict with stored evidence aborts the range); source operations whose record is absent (an existing record is never replaced); the legacy projection, which only moves a pending or stuck operation to completed using the same completing events as live indexing (Neo X deposit, claimable and claim events, and a message root update for its own nonce; N3 distribution, claim and execution), and lets a recovered operation take up a legacy deferred completion as live reconciliation does. A recovered operation with no destination evidence yet that is older than the configured stuck threshold (`STUCK_THRESHOLD_*`) is stored as legacy `stuck`: the stuck detector would otherwise move it there on its next pass and send one stale alert per historical operation. Younger ones stay `pending`. Neo X to N3 messages stay `pending`, as live. Its lifecycle stays unknown, and destination evidence found later completes it; directional states, which only rise, with a verified received root replacing same-nonce synthetic bookkeeping; the range's Neo X claim trace gaps (a traced block also resolves an earlier live gap for it) and the N3 claims still lacking a verified pair, which replace earlier unresolved records inside the range; and the watermark. Settlement is never written as execution, distribution or claim. The live checkpoint is never written. Every fact in a range must be on the replayed chain and inside the range.

**Manifests created before event-only recovery.** A Neo X manifest begun with the earlier version requires the `transactions` and `claims` paths, and its paths and implementation descriptions cannot change in place (`begin` refuses changed bounds). `recover run` on such a manifest fails early unless `--claim-scan` is given, and continues it block by block. To switch to event-only recovery, start a new manifest on a fresh database copy. Production recovery has not been started, so no production manifest is affected.

**Neo X claim reconciliation.** `claimToken` emits no bridge log in any deployed version, so bridge logs alone never prove claim coverage. In every version, however, a token claimable is created only together with a `TokenClaimable` log, and `claimToken` succeeds only by deleting that record from `tokenClaimables[token][nonce]` (storage slot 104, verified on mainnet). `recover claims` therefore checks each token claimable that has no claim evidence through H:

- If the record is still held at H, the claimable is outstanding: no claim of it happened through H. This is coverage only; no `not_claimed` fact is written.
- If the record is gone, a binary search over historical `eth_getStorageAt` finds the block that consumed it. That one block is decoded for the direct `claimToken` transaction (implementation 1.0.0, 2.0.0 or 3.0.0), and traced if the claim was indirect. The claim is recorded with its transaction, block and call position, and the legacy record completes as for a live claim.
- If the claim still cannot be identified (an indirect call without a trace, an unverified implementation, or no record at the verified slot), it is stored as an unresolved claim with the consuming height, and never as unclaimed.

Native claims and message executions emit logs, so event-only ranges cover them. Reconciliation commits atomically. It clears every claim gap (`claims_deferred` and trace gaps) in `[start, H]` only when the recovery started at the Bridge's deployment (Neo X genesis, `--start 0`) and no claimable is unresolved, because only then is every claimable known. A later replay or extension needs a new `recover claims`. On mainnet the replay found exactly one token claimable (nonce 108). Its record is cleared at 6,757,004 by the direct claim `0xeba5f840…4ab4`, which reconciliation identifies with 17 storage reads and one block.

**Completion.** `complete` requires the manifest to be contiguous from start through the current target with the target hash matched, no Neo X claim gap (deferred or trace), no unresolved Neo X token claim and no unresolved N3 claim inside the bounds. Event-only recovery is therefore never complete or `handoff ready` until `recover claims` has cleared the deferred claims. `handoff ready` additionally requires the live checkpoint to equal the target, so the live indexer resumes at the first block after the verified interval: no block is uncovered and none is processed twice. Recovery never moves the checkpoint; instead the target is extended to it (`extend --target checkpoint`). A target beyond the checkpoint (for example `head` in a rehearsal) is not ready until live indexing has passed it and the target is extended again. A public Neo X endpoint without `debug_traceBlockByNumber` can recover every event, but leaves a gap for each block with transactions; completion then needs a tracing endpoint for those blocks (`recover` with a tracing endpoint over the same interval, or the live enrichment).

**Live interaction.** The server and `recover` both open the database read-write, so Badger's directory lock lets only one run at a time. They can alternate: stop the server, run a bounded `recover run`, restart the server. Because every recovery write is insert-only or monotonic and conflicts abort, alternating never regresses live state, and live indexing after recovery accepts the same facts again.

**N3 event filter plugin.** `recover begin --chain neo_n3 --event-filter` records the [AxLabs neo-event-filter-plugin](https://github.com/AxLabs/neo-event-filter-plugin) (reviewed at `651904a`) as the manifest's trusted index. `recover run` then probes `NEO_N3_RPC_ENDPOINTS` with `geteventfilterstatus` and sends plugin queries only to the first endpoint that answers. Backups without the plugin keep serving every standard call. Inside the plugin's live-indexed window, `[liveIndexedFrom, lastIndexedBlock]`, a range of up to `--located-batch` blocks (default 10000) is located with `geteventfilterlogs` for the two bridge contracts. Only the listed blocks and the range boundaries are fetched, and they are decoded from `getblock` and `getapplicationlog` exactly as in a full scan. The plugin is never used as evidence: it does not record VM state, its notification index is per block rather than per transaction position, and its payload encoding differs. Ranges below `liveIndexedFrom` are always scanned fully, because the plugin builds that part of its index by re-executing old transactions against the current state ("best-effort reconstruction"), which can produce different bridge nonces and roots or drop blocks whose re-execution fails. Before using it, each run applies a positive control: the plugin must list a bridge event in its live window, and that transaction's real application log and height on the same node must contain a HALT notification from the bridge. This rejects a plugin that answers every query with an empty page, for example one with a non-positive result cap. Each located range also requires the plugin node's `getblockhash` at the range end to equal the hash recovery verified, so the index belongs to the chain being recovered. An unavailable plugin, a failed control or query, a hash mismatch, or a window that changes during a query falls back to a full scan; it never blocks recovery. Trust assumption: located blocks (`located_blocks` in `status`) are covered on the plugin's word, and `status` then reports `complete (index-trusted)`. Its metadata cannot show a gap in live indexing (for example while the plugin was disabled, or a block whose indexing failed and was only logged), so a missed bridge notification there would go unnoticed. Use it only on a node that has run the plugin continuously since `liveIndexedFrom`, and keep the manifest's trusted index in mind before treating absence as `not_executed`. The live N3 indexer does not use the plugin.

**Throughput.** Measured against public mainnet nodes on November 25, 2026, from a disposable database:
- Neo X event-only replay from genesis through 8,412,377 took 22 min 41 s (about 6,200 blocks/s, 132 MB peak memory), with no RPC errors at concurrency 1. It committed 85 ranges, 6,104 observations and 2,429 recovered source operations, including the pre-3.0.0 `Gas*` history.
- `recover claims` then took 9 s. It identified the only token claim (nonce 108), left 0 outstanding and 0 unresolved, and cleared all claim gaps, so `status` reports `complete`.
- The per-block `--claim-scan` path runs at about 8 blocks/s serially (about 12 days for the same history).
- N3 runs at about 21 blocks/s with `N3_SYNC_CONCURRENCY=8`; it has no `eth_getLogs` and is scanned block by block unless the event-filter plugin's live window applies.

**Source history (frontend #19).** The native history that stopped at nonce 1317 was emitted under the pre-3.0.0 `Gas*` names, which earlier decoders ignored (an earlier version of this document wrongly said those deposits had no Neo X destination event). Neo X holds `GasDeposit`/`GasDepositRootUpdate` for nonces 1–1316 and `GasWithdrawal` for 1–440. N3 holds `GasDeposit` (first at 5,752,006) and `GasWithdrawalRootUpdate`/`GasWithdrawal` (first at 5,752,312). Replaying both chains recovers these operations with their destination outcomes.

**Runbook.**

1. Rehearse on a copy: copy the stopped database, then on the copy run `begin` for each chain with `--start` at that chain's bridge deployment height (Neo X `0`, N3 `5751638`) and `--target head`, `run` to completion, `recover claims --chain neo_x`, and `status`. Record durations, gap blocks and unresolved claims. Check the incident examples below in the copy's API. Discard the copy; its progress does not carry over.
2. Production, possibly in several maintenance windows: stop the server, back up the database, `begin` with the same start heights and `--target checkpoint`, then `run --max-blocks N` sized to the window, and restart the server. Repeat `run` in later windows; the manifest persists.
3. Cutover: stop the server, back up, `extend --target checkpoint` on each chain (the live indexer advanced meanwhile), `run` to the target, `recover claims --chain neo_x`, then `status`. Restart only when each chain is `handoff ready`. If Neo X claims stay unresolved (for example an indirect claim without a tracing endpoint) and that limitation is accepted, those claim outcomes stay unknown, not `not_executed`.
4. Rollback: stop the server and restore the backup. Recovery writes are additive and keyed, so a partial recovery can also be resumed instead.


## Contract evidence

The mappings were checked on November 23, 2026 against these pinned official source revisions. Historical event decoding must also match the ABI of the contract implementation at the event's block.

- Neo X native and token events and relay implementations. [INativeBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/INativeBridge.sol), [ITokenBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/ITokenBridge.sol), [BridgeImpl](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/bridge/BridgeImpl.sol)
- Neo X message events, execution behavior, and metadata/result definitions. [IMessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/interfaces/IMessageBridge.sol), [MessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/MessageBridge.sol), [AMBTypes](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/library/AMBTypes.sol)
- N3 received-root updates, immediate distribution, claimable outcomes, and claiming. [NativeBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/NativeBridgeImpl.java), [TokenBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/TokenBridgeImpl.java)
- N3 message root updates, per-message Store events, and chunked execution results. [MessageBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/message/MessageBridgeImpl.java)
