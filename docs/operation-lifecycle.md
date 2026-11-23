# Settlement and execution

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

Recovery tests must include a destination outcome added after the rehearsal snapshot but before production cutover, an interrupted replay with a normal checkpoint ahead of its recovery watermark, and a failed trace or block fetch inside an otherwise complete range. Each incomplete case must keep the new negative projection and operation-stuck alerts unavailable. Only a verified contiguous replay and handoff may enable them. Exercise both chains with different numeric height ranges.

Retries must be idempotent. Rebuild directional state from real source and received observations. A copy-and-replay comparison must produce the same lifecycle as live processing, including when destination events precede source indexing. Report unresolved records and incomplete ranges. They block production promotion.

The known incident checks from the November 20, 2026 investigation are acceptance examples, rather than fixed live values.

- Message Neo X to N3 had source nonce 15 and indexed destination nonce 13. N3 `evmToNeoNonce` returned 15 and `evmToNeoRoot` matched the source root `bd8a3be21d32d769c8f80e7f803166aa7c29bdfb72a315aabb3678869eb324f6`. Recover the covering receipt and per-message storage independently of execution.
- NDMEME deposit nonce 27 had N3 source height `13051516` and a Neo X `TokenDeposit` observation in transaction `0xc34ba5e2efcc32a5c6b317c87e8ad7b4e5e2113e9a0eb333e6f79cc8a4f6cdb2`, log index 2, block `0x74acee`. Topic zero was `0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b`, token `0xe816dee05cf6d0f2a57eb4c489241d8326b5d106`, nonce 27. The recovery regression must find this lower-numbered destination block using Neo X bounds. Starting a Neo X scan at N3 height `13051516` must fail the test. Recover arrival and distribution and verify the history and direction. [Frontend #22](https://github.com/bane-labs/bridge-indexer-frontend/issues/22)
- NEO withdrawal nonce 11 was reported distributed in N3 transaction `0x1c4c374824b8059230e3bd1dc8c780294531bed4c591de72032119372a79c1f8`. Retrieve and validate the actual notification set before using it as a regression fixture or closing the issue. [Frontend #16](https://github.com/bane-labs/bridge-indexer-frontend/issues/16)

The persistence and decoder PRs must cover immediate distribution, claimable then claimed, stored and executable messages, committed execution results, reverted attempts, chunked events, multiple token pairs at equal nonces, retries, and out-of-order processing. Preserve the legacy response and test the new frontend through its API request path with reusable fixtures, MSW, and providers.

Before production, back up the database and rehearse migration and rollback on a copy. Recover and verify the backend first, verify both API projections, then deploy the frontend. Retain the original database until verification completes. Production deployment requires separate authorization. Recovering missing source history is covered separately by frontend #19.

## Contract evidence

The mappings were checked on November 23, 2026 against these pinned official source revisions. Historical event decoding must also match the ABI of the contract implementation at the event's block.

- Neo X native and token events and relay implementations. [INativeBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/INativeBridge.sol), [ITokenBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/ITokenBridge.sol), [BridgeImpl](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/bridge/BridgeImpl.sol)
- Neo X message events, execution behavior, and metadata/result definitions. [IMessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/interfaces/IMessageBridge.sol), [MessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/MessageBridge.sol), [AMBTypes](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/library/AMBTypes.sol)
- N3 received-root updates, immediate distribution, claimable outcomes, and claiming. [NativeBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/NativeBridgeImpl.java), [TokenBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/TokenBridgeImpl.java)
- N3 message root updates, per-message Store events, and chunked execution results. [MessageBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/message/MessageBridgeImpl.java)
