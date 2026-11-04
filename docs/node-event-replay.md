# AUXP node event replay in Command

Command negotiates `execution.activity.v1` and `node.event_replay.v1` from the
authenticated node's `node.hello` and returns only the supported intersection.
The accepted list is stored before Command acknowledges the hello and is
reloaded with node state after a restart. A node without the replay capability
keeps the existing live event behavior; Command does not send it replay or ACK
commands.

Every authenticated notification is recorded by organization, node, sequence,
event ID, method and a SHA-256 digest of its normalized params (excluding the
delivery-specific `messageId`); unprojected raw payloads are not retained.
Organization and node identity are bound to the
authenticated credential or WebSocket and overwrite the values in the message
before persistence. Exact retries are idempotent. Reusing a new
event ID at an existing sequence, changing the payload for an existing event,
or reusing an event ID at another sequence is rejected. For replay-capable
nodes, a sequence gap is rejected without moving the cursor. Legacy nodes may
deliver out of order; Command stages those records but only advances the ACK
cursor when every sequence from the prior cursor is present.

For projected event types, the identity/digest ledger row, contiguous cursor
and Office event projection commit in the same PostgreSQL transaction. An ACK
is sent on the authenticated WebSocket only after that commit and only through
the highest contiguous sequence. If a message arrives over HTTP without an open command
connection, its successful HTTP response follows the durable commit; the next
`node.resume` carries that stored cursor.

Command currently retains the receiver ledger indefinitely; there is no Command
retention or pruning setting for these rows. Migration 013 seeds the cursor
from previously durable node and Office cursors. Payloads predating the ledger
cannot be verified against an old event ID, so such sequences are treated as
already acknowledged. New rows receive full duplicate and conflict checks.

On a WebSocket reconnect, Command sends `node.resume` with its durable cursor and
validates the returned replay metadata. If the node reports `cursor_expired`,
Command requests `run.snapshot` for runs currently known in that node's
registry and replaces those cached run summaries. It does not pretend that a
run snapshot recreates missing activity, audit, or Office timeline events.
Those historical entries remain unavailable. Command does not advance the
node-wide cursor or ACK across the gap: `run.snapshot` is per-run and the
current protocol has no atomic node-wide snapshot cursor that can prove every
run and event stream is covered. Core #80 must supply that checkpoint contract
before Command can safely resume after an expired cursor without operator
intervention.
