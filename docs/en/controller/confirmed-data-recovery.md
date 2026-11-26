# Replacing a replica after a confirmed-offset conflict

In controller mode, a replica refuses a handshake that would truncate its log below its local
`confirmOffset`. The refusal is saved beside `storePathEpochFile`, with the suffix
`.confirmed-offset-guard`. Reconnecting, restarting, changing the master address or receiving a
larger offset does not remove it. A damaged guard record stops initialization instead of enabling
replication.

If saving a refusal fails, the running follower still refuses handshakes and retries saving the same
evidence. Resolve the storage failure before restarting it: evidence which never reached durable
storage cannot survive process loss. A nonempty CommitLog without epoch history also fails HA
initialization instead of becoming a source with an invented history.

A matching physical offset does not establish matching message content. In particular, after a
master loses data and writes replacements under the same epoch, the first refused offset may be
later than the actual divergence. Do not delete the guard or reset the confirmation checkpoint to
force that replica to join.

## Upgrade order and restart behavior

Coordinate the upgrade across the controller group and its brokers. Upgrade every controller before
starting an upgraded broker, and upgrade every broker in the affected group before relying on the
new startup-epoch behavior. The guarantee does not apply to mixed-version deployments.

Before upgrading, take consistent backups of the controller snapshots and logs and the corresponding
broker stores and identities. After accepting a new startup registration, do not downgrade either
component in place over the newer state. Older controllers do not apply the same startup metadata,
and older brokers do not honor the new refusal guard or startup registration. Rollback requires
coordinated recovery from compatible controller and broker backups and reconciliation of any writes
since those backups.

Each broker startup persists a monotonically increasing generation beside `storePathBrokerIdentity`
with the suffix `-startup`, and uses a fresh startup ID for registration. The controller persists the
accepted pair. A retry with the same pair is idempotent. An older generation, or a reused generation
with another startup ID, is refused. An upgraded broker stays fenced if the controller does not
acknowledge its startup identity.

When the current master restarts, the controller grants a new master epoch without reducing the
SyncStateSet. The broker durably records that epoch at the recovered log end before enabling writes.
Thus replacement messages cannot extend the previous epoch. A follower restart does not advance
the group's master epoch. Duplicate grants in one startup retain the original epoch boundary.

Keep the broker identity and its startup metadata together. Restoring an old startup counter is not
a supported way to rejoin an existing broker identity; the controller may correctly refuse it.
Use a new replica identity for a replacement store.

The new startup behavior does not prove that replicas which diverged before the upgrade contain
identical history. Validate or replace those replicas before returning the group to service.

## Choose the authoritative history

First stop the affected writer and preserve every potentially useful store. Record the broker group,
broker identities, master epoch, epoch files, confirmation checkpoints and refusal records. Compare
actual messages where the histories differ. Prefer a healthy replica retaining the acknowledged
messages. If that history can be selected as master, use it as the source for the replacement.

If the operator deliberately accepts an unclean election, the selected master may not have all
messages acknowledged by the old master. That choice must be explicit. A fresh replica can restore
redundancy for the verified history on the selected master, but it cannot bring across messages that
master no longer has. Preserve the original store and refusal record until any missing confirmed
history is reconciled; a healthy replacement alone does not complete that work.

The existing election policy is unchanged. The new epoch separates histories and allows replicas to
reject an unsafe rollback; it does not automatically choose the replica containing the most data.
If the available election is unsafe, a group can remain unavailable until the operator selects and
repairs the appropriate history.

## Restore redundancy using a fresh replica

Use a fresh replica when a common prefix has not been established. This procedure does not rejoin
the refused member, clear its guard, or infer a rollback point from its first refused handshake.

1. Stop the refused replica. Retain its complete store, including CommitLog, ConsumeQueue, epoch and
   checkpoint files and the refusal record. Do not reuse any of those log or queue files in the new
   directory. If paths were configured outside `storePathRootDir`, preserve them as well.
2. Select the authoritative master and check that it retains all data required for recovery. Record
   its identity and epoch. Keep that source stable during recovery; pause the operation and reassess
   the choice if its history changes. Do not enable unclean elections merely to advance the rebuild.
3. Configure a new, empty store directory and a new broker identity in the same broker group. Set
   `syncFromLastFile=false`. Ensure custom CommitLog, ConsumeQueue, epoch and identity paths do not
   point back to the preserved store. Follow the existing controller deployment instructions for
   registering the new replica; do not copy the old `brokerIdentity` or `-startup` files into it.
4. Start the replacement as a follower of the selected group. It copies the master's retained
   CommitLog; it does not reconstruct expired or missing history. Keep the old replica stopped, so its
   address and broker identity cannot be reused concurrently.
5. Check actual message contents and queue order over the required range, the epoch history,
   `maxOffset`, `confirmOffset`, checkpoint and ConsumeQueue progress. Confirm that new messages also
   replicate. Equal offsets alone are insufficient. Recheck after a normal restart of the new
   replica before considering the replacement complete.
6. Retire the old stopped identity using the existing controller administration procedure only after
   the replacement is healthy. Retiring the identity does not remove the need to retain and reconcile
   the preserved data and refusal record.

The fresh directory is the explicit choice to adopt the selected history. There is no reusable
authorization flag that can silently permit a later loss of confirmed data. Interrupting the copy
leaves the preserved old store unchanged; continue the replacement only while the selected history
remains valid. If the selected source has already expired required log files, restore an appropriate
backup or obtain another retained source before proceeding.

A healthy holder of protected data can still be promoted. A refusal of another history is not
evidence that this holder's own data is corrupt. When pre-upgrade same-epoch divergence is known,
keep unverified old sources and their descendants isolated until they have been rebuilt; the new
startup epoch mechanism cannot retroactively certify their contents.
