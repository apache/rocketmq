# Recover an isolated controller-mode replica

When a handshake would truncate below the replica's confirmed offset, the replica refuses replication and records the incident in `ha-history-incident` under its `storePathRootDir`. Later handshakes keep the earliest recorded cut even if the elected master writes past the old confirmed offset in the same epoch. Both master-promotion paths check the incident. Enabling unclean election does not authorize local data loss.

## Accept the elected master's history once

On receipt of a complete handshake, a healthy replica records its candidate cut and local confirmation in `ha-history-received` before acting on the comparison. A restart replays a completed record. An incomplete or unreadable journal blocks the replica. Include this file in incident backups when present.

1. Identify the **isolated replica**, often the former master. Keep the elected master as the source. Stop the isolated broker, take a restorable backup of its complete Store (including configured external commit-log and epoch paths), and preserve its broker identity.
2. On the stopped replica inspect `ha-history-incident`. Its lines are the unique incident ID, first byte to discard, confirmed offset at detection, original local end, selected HA endpoint followed by its ordered `epoch@start` entries, and a SHA-256 checksum. Confirm the elected master's identity independently. Verify the retained prefix against a trusted copy of the selected history using actual commit-log bytes. The endpoint, epochs and offsets identify the operator's choice and provide no payload comparison. If the prefix cannot be validated or the cut predates retained data, rebuild the replica from a trusted backup of the elected history.
3. Approving this record discards **every local byte from the cut to the local end**, including confirmed messages. For example, with a recorded cut of 1580 and former confirmed end of 3280, messages in `[1580, 3280)` will be replaced even if the elected master has already reached 4190. Run the following from the stopped replica's Store root after inspecting its record.

```sh
python3 - <<'PY'
import os
from pathlib import Path
import tempfile
root = Path.cwd()
record = (root / "ha-history-incident").read_bytes()
with tempfile.NamedTemporaryFile(dir=root, delete=False) as staged:
    staged.write(record)
    staged.flush()
    os.fsync(staged.fileno())
os.replace(staged.name, root / "ha-history-approval")
directory = os.open(root, os.O_RDONLY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
```

4. Restart the isolated replica as a slave of the selected master. A matching handshake changes the nonce, consumes the approval durably, and then truncates the Store. The incident clears after the retained commit-log segment, deleted segment directories, epoch checkpoint and confirmed offset have been persisted. Verify that the replica catches up and compare the replacement messages with the source before returning it to service.

## Withdraw approval or handle interruption

To withdraw an unused approval, stop the replica and run the following in its Store root. Restart it as a slave. A consumed approval cannot undo truncation. Preserve the incident file until repair completes.

```sh
python3 - <<'PY'
import os
from pathlib import Path
approval = Path("ha-history-approval")
if approval.exists():
    approval.unlink()
directory = os.open(".", os.O_RDONLY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
```

If the endpoint, epoch sequence or cut changes, the replica issues a fresh incident ID. A healthy replica can restart while waiting for its first response and reconnect normally. An already-isolated replica creates `ha-history-inspecting` before requesting history again. If it crashes or loses storage while inspecting, it may have seen a smaller cut. Its next usable handshake invalidates the old approval and proposes a cut of **zero**. Preserve both incident files. After an interrupted repair, inspect the current incident and disk state and approve the new record only if the remaining repair is still intended. An unreadable or damaged record, unsupported durable filesystem operation, or lack of a common epoch keeps the replica isolated. Restore a trusted Store snapshot or rebuild when the retained prefix or storage integrity cannot be established.
