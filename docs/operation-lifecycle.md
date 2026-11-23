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

The monitoring threshold applies to missing arrival. An arrived operation awaiting a user claim or message execution cannot become relay-stuck. Keep the current message-withdrawal alert exemption during this rollout. General liveness alerting remains covered by backend #15 and its follow-up.

## Event mapping

The names below come from the official contract interfaces and implementations linked under [Contract evidence](#contract-evidence). Associate every event with its emitting contract, chain, direction, and token pair before updating state. Only events in committed transactions establish observations.

### Neo N3 to Neo X

| Chain and event | Fact |
| --- | --- |
| N3 `NativeDeposit` / `TokenDeposit` | Source request, source nonce/root and token-pair identity |
| Neo X `NativeDepositRootUpdate` / `TokenDepositRootUpdate` | Destination received nonce/root and relay transaction/time |
| Neo X `NativeDeposit` / `TokenDeposit` | Distribution for the operation in the relay transaction, execution recorded |
| Neo X `NativeClaimable` / `TokenClaimable` | Arrival with funds available to claim, execution outstanding |
| Neo X `NativeClaim` / `TokenClaim` | Execution through claiming, original settlement retained |
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
- Combine N3 execution chunks by transaction and nonce, validating the declared chunk count and indices. Preserve the payload and create one execution observation.
- A recognized event with invalid fields, an unavailable required application log, or a failed persistence operation must prevent checkpoint advancement for that work unit. Unrelated events can be ignored. A transaction-script prefilter must not discard indirect calls that emit watched events.

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

Unknown message types or incomplete historical coverage must not be guessed. Publish the new lifecycle projection only when the record has sufficient decoded provenance and recovery coverage to establish it. Otherwise omit the whole projection and retain the legacy fields. A missing event during an incomplete historical scan is not evidence that execution did not happen. Once recovery is complete, absence of an execution observation establishes `not_executed` for an applicable operation.

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

Retries must be idempotent. Rebuild directional state from real source and received observations. A copy-and-replay comparison must produce the same lifecycle as live processing, including when destination events precede source indexing. Report unresolved records and incomplete ranges. They block production promotion.

The known incident checks from the November 20, 2026 investigation are acceptance examples, rather than fixed live values.

- Message Neo X to N3 had source nonce 15 and indexed destination nonce 13. N3 `evmToNeoNonce` returned 15 and `evmToNeoRoot` matched the source root `bd8a3be21d32d769c8f80e7f803166aa7c29bdfb72a315aabb3678869eb324f6`. Recover the covering receipt and per-message storage independently of execution.
- NDMEME deposit nonce 27 had a Neo X `TokenDeposit` observation in transaction `0xc34ba5e2efcc32a5c6b317c87e8ad7b4e5e2113e9a0eb333e6f79cc8a4f6cdb2`, log index 2, block `0x74acee`. Topic zero was `0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b`, token `0xe816dee05cf6d0f2a57eb4c489241d8326b5d106`, nonce 27. Recover arrival and distribution and verify the history and direction. [Frontend #22](https://github.com/bane-labs/bridge-indexer-frontend/issues/22)
- NEO withdrawal nonce 11 was reported distributed in N3 transaction `0x1c4c374824b8059230e3bd1dc8c780294531bed4c591de72032119372a79c1f8`. Retrieve and validate the actual notification set before using it as a regression fixture or closing the issue. [Frontend #16](https://github.com/bane-labs/bridge-indexer-frontend/issues/16)

The persistence and decoder PRs must cover immediate distribution, claimable then claimed, stored and executable messages, committed execution results, reverted attempts, chunked events, multiple token pairs at equal nonces, retries, and out-of-order processing. Preserve the legacy response and test the new frontend through its API request path with reusable fixtures, MSW, and providers.

Before production, back up the database and rehearse migration and rollback on a copy. Recover and verify the backend first, verify both API projections, then deploy the frontend. Retain the original database until verification completes. Production deployment requires separate authorization. Recovering missing source history is covered separately by frontend #19.

## Contract evidence

The mappings were checked on November 23, 2026 against these pinned official source revisions. Historical event decoding must also match the ABI of the contract implementation at the event's block.

- Neo X native and token events and relay implementations. [INativeBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/INativeBridge.sol), [ITokenBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/interfaces/ITokenBridge.sol), [BridgeImpl](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/bridge/BridgeImpl.sol)
- Neo X message events, execution behavior, and metadata/result definitions. [IMessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/interfaces/IMessageBridge.sol), [MessageBridge](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/messageBridge/MessageBridge.sol), [AMBTypes](https://github.com/bane-labs/bridge-evm-contracts/blob/a70b698be02bda605b2ba1df02248b99574f85b8/contracts/library/AMBTypes.sol)
- N3 received-root updates, immediate distribution, claimable outcomes, and claiming. [NativeBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/NativeBridgeImpl.java), [TokenBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/bridge/TokenBridgeImpl.java)
- N3 message root updates, per-message Store events, and chunked execution results. [MessageBridgeImpl](https://github.com/bane-labs/bridge-neo-contracts/blob/9a697e3d908a89d0076d2b82c2d5587bcf7b306c/src/main/java/network/bane/message/MessageBridgeImpl.java)
