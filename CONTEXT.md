# Bridge lifecycle

The bridge carries native assets, tokens, and messages between Neo N3 and Neo X. Arrival on the destination chain and the operation's final effect are separate observations.

## Language

**Directional instance**
A bridge type and source-to-destination chain direction, including both token contracts for a token bridge. Its nonce sequence belongs to that instance.

**Nonce**
An operation's sequence number within its directional instance. The instance and nonce together identify the operation across source and destination observations.

**Deposit**
An operation requested on Neo N3 for delivery to Neo X.

**Withdrawal**
An operation requested on Neo X for delivery to Neo N3.

**Settlement**
An operation's arrival on the destination chain, also called relaying. It establishes receipt independently of a later claim or message execution.
_Avoid_ treating settlement as successful completion of every destination action.

**Execution**
The destination contract's recorded execution of an applicable operation. For assets this is distribution or claiming, and for executable messages this is message execution with its recorded result.

**Distribution**
Delivery of the bridged assets to the recipient during the relay transaction.

**Claimable**
An arrived asset operation whose funds await a successful claim.

**Claim**
The destination transaction that delivers funds from a previously claimable asset operation.

**Executable message**
A message that can be executed after arrival. Its recorded execution result may report that a target action failed.

**Store-only message**
A message delivered for storage with no execution requirement.

**Result message**
A message carrying a previous message's execution result, with no execution requirement of its own.

**Directional sync**
Agreement between the source's requested nonce/root and the destination's received nonce/root. Operations awaiting a claim or execution can belong to a synced direction.

**Observation**
An on-chain event or verified successful bridge call establishing a lifecycle fact and its chain, block, transaction, position, and time.

**Relay overdue**
An applicable operation whose arrival has not been observed within the monitoring threshold. Waiting for a claim or execution after arrival does not establish relay delay.
