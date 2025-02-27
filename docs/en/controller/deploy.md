# Deployment and upgrade guidelines

## Controller deployment 

 If the controller needs to be fault-tolerant, it needs to be deployed in three or more replicas (following the Raft majority protocol).

> Controller can also complete Broker Failover with only one deployment, but if the single point Controller fails, it will affect the switching ability, but will not affect the normal reception and transmission of the existing cluster.

There are two ways to deploy Controller. One is to embed it in NameServer for deployment, which can be opened through the configuration enableControllerInNamesrv (it can be opened selectively and is not required to be opened on every NameServer). In this mode, the NameServer itself is still stateless, that is, if the NameServer crashes in the embedded mode, it will only affect the switching ability and not affect the original routing acquisition and other functions. The other is independent deployment, which requires separate deployment of the controller.

### Embed NameServer deployment

When embedded in NameServer deployment, you only need to set `enableControllerInNamesrv=true` in the NameServer configuration file and fill in the controller configuration.

```
enableControllerInNamesrv = true
controllerDLegerGroup = group1
controllerDLegerPeers = n0-127.0.0.1:9877;n1-127.0.0.1:9878;n2-127.0.0.1:9879
controllerDLegerSelfId = n0
controllerStorePath = /home/admin/DledgerController
enableElectUncleanMaster = false
notifyBrokerRoleChanged = true
```

Parameter explain：

- enableControllerInNamesrv: Whether to enable controller in Nameserver, default is false.
- controllerDLegerGroup: The name of the DLedger Raft Group, all nodes in the same DLedger Raft Group should be consistent.
- controllerDLegerPeers: The port information of the nodes in the DLedger Group, the configuration of each node in the same Group must be consistent.
- controllerDLegerSelfId: The node id, must belong to one of the controllerDLegerPeers; unique within the Group.
- controllerStorePath: The location to store controller logs. Controller is stateful and needs to rely on logs to recover data when restarting or crashing, this directory is very important and should not be easily deleted.
- enableElectUncleanMaster: Whether it is possible to elect Master from outside SyncStateSet, if true, it may select a replica with lagging data as Master and lose messages, default is false.
- notifyBrokerRoleChanged: Whether to actively notify when the role of the broker replica group changes, default is true.

Some other parameters can be referred to in the ControllerConfig code.

After setting the parameters, start the Nameserver by specifying the configuration file.

### Independent deployment

To deploy independently, execute the following script:

```shell
sh bin/mqcontroller -c controller.conf
```
The mqcontroller script is located at distribution/bin/mqcontroller, and the configuration parameters are the same as in embedded mode.

## Broker Controller mode deployment

The Broker start method is the same as before, with the following parameters added:

- enableControllerMode: The overall switch for the Broker controller mode, only when this value is true will the controller mode be opened. Default is false.
- controllerAddr: The address of the controller, separated by semicolons if there are multiple controllers. For example, `controllerAddr = 127.0.0.1:9877;127.0.0.1:9878;127.0.0.1:9879`
- syncBrokerMetadataPeriod: The interval for synchronizing Broker replica information with the controller. Default is 5000 (5s).
- checkSyncStateSetPeriod: The interval for checking SyncStateSet, checking SyncStateSet may shrink SyncState. Default is 5000 (5s).
- syncControllerMetadataPeriod: The interval for synchronizing controller metadata, mainly to obtain the address of the active controller. Default is 10000 (10s).
- haMaxTimeSlaveNotCatchup: The maximum interval that a slave has not caught up to the Master, if a slave in SyncStateSet exceeds this interval, it will be removed from SyncStateSet. Default is 15000 (15s).
- storePathEpochFile: The location to store the epoch file. The epoch file is very important and should not be deleted arbitrarily. Default is in the store directory.
- allAckInSyncStateSet: If this value is true, a message needs to be replicated to each replica in SyncStateSet before it is returned to the client as successful, ensuring that the message is not lost. Default is false.
- syncFromLastFile: If the slave is a blank disk start, whether to replicate from the last file. Default is false.
- asyncLearner: If this value is true, the replica will not enter SyncStateSet, that is, it will not be elected as Master, but will always be a learner replica that performs asynchronous replication. Default is false.
- inSyncReplicas: The number of replica groups that need to be kept in sync, default is 1, inSyncReplicas is invalid when allAckInSyncStateSet=true.
- minInSyncReplicas: The minimum number of replica groups that need to be kept in sync, if the number of replicas in SyncStateSet is less than minInSyncReplicas, putMessage will return PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH directly, default is 1.

In Controller mode, the Broker configuration must set `enableControllerMode=true` and fill in controllerAddr.

### Analysis of important parameters

Among the parameters such as inSyncReplicas and minInSyncReplicas, there are overlapping and different meanings in normal Master-Slave deployment, SlaveActingMaster mode, and automatic master-slave switching architecture. The specific differences are as follows:

|                      | inSyncReplicas                                                      | minInSyncReplicas                                                        | enableAutoInSyncReplicas                    | allAckInSyncStateSet                                          | haMaxGapNotInSync                     | haMaxTimeSlaveNotCatchup                          |
|----------------------|---------------------------------------------------------------------|--------------------------------------------------------------------------|---------------------------------------------|---------------------------------------------------------------|---------------------------------------|---------------------------------------------------|
| Normal Master-Slave deployment | The number of replicas that need to be ACKed in synchronous replication, invalid in asynchronous replication | invalid                                                                | invalid                                   | invalid                                                     | invalid                             | invalid                                         |
| Enable SlaveActingMaster （slaveActingMaster=true） | The number of replicas that need to be ACKed in synchronous replication in the absence of auto-degradation | The minimum number of replicas that need to be ACKed after auto-degradation | Whether to enable auto-degradation, and the minimum number of replicas that need to be ACKed after auto-degradation is reduced to minInSyncReplicas | invalid                                                     | Basis for degradation determination: the difference in Commitlog heights between Slave and Master, in bytes | invalid                                         |
| Automatic master-slave switching architecture（enableControllerMode=true） | The number of replicas that need to be ACKed in synchronous replication when allAckInSyncStateSet is not enabled, and this value is invalid when allAckInSyncStateSet is enabled | SyncStateSet can be reduced to the minimum number of replicas, and if the number of replicas in SyncStateSet is less than minInSyncReplicas, it will return directly with insufficient number of replicas | invalid                                   | If this value is true, a message needs to be replicated to every replica in SyncStateSet before it is returned to the client as successful, and this parameter can ensure that the message is not lost | invalid                      | The minimum time difference between Slave and Master when SyncStateSet is contracted, see [RIP-44](https://shimo.im/docs/N2A1Mz9QZltQZoAD) for details. |

To summarize：
- In a normal Master-Slave configuration, there is no ability for auto-degradation, and all parameters except for inSyncReplicas are invalid. inSyncReplicas indicates the number of replicas that need to be ACKed in synchronous replication.
- In slaveActingMaster mode, enabling enableAutoInSyncReplicas enables the ability for degradation, and the minimum number of replicas that can be degraded to is minInSyncReplicas. The basis for degradation is the difference in Commitlog heights (haMaxGapNotInSync) and the survival of the replicas, refer to  [SlaveActingMaster mode adaptive degradation](../QuorumACK.md).
- Automatic master-slave switching (Controller mode) relies on SyncStateSet contraction for auto-degradation. SyncStateSet replicas can work normally as long as they are contracted to a minimum of minInSyncReplicas. If it is less than minInSyncReplicas, it will return directly with insufficient number of replicas. One of the basis for contraction is the time interval (haMaxTimeSlaveNotCatchup) at which the Slave catches up, rather than the Commitlog height. If allAckInSyncStateSet=true, the inSyncReplicas parameter is invalid.

## Compatibility

This mode does not make any changes or modifications to any client-level APIs, and there are no compatibility issues with clients.

The Nameserver itself has not been modified and there are no compatibility issues with the Nameserver. If enableControllerInNamesrv is enabled and the controller parameters are configured correctly, the controller function is enabled.

If Broker is set to **`enableControllerMode=false`**, it will still operate as before. If **`enableControllerMode=true`**, the Controller must be deployed and the parameters must be configured correctly in order to operate properly.

The specific behavior is shown in the following table:

|                                    | Old nameserver                  | Old nameserver + Deploy controllers independently | New nameserver enables controller | New nameserver disable controller |
| ---------------------------------- | ------------------------------- | ------------------------------------------------- | --------------------------------- | --------------------------------- |
| Old broker                         | Normal running, cannot failover | Normal running, cannot failover                   | Normal running, cannot failover   | Normal running, cannot failover   |
| New broker enable controller mode  | Unable to go online normally    | Normal running, can failover                      | Normal running, can failover      | Unable to go online normally      |
| New broker disable controller mode | Normal running, cannot failover | Normal running, cannot failover                   | Normal running, cannot failover   | Normal running, cannot failover   |

## Upgrade Considerations

From the compatibility statements above, it can be seen that NameServer can be upgraded normally without compatibility issues. In the case where the Nameserver is not to be upgraded, the controller component can be deployed independently to obtain switching capabilities. For broker upgrades, there are two cases:

1. Master-Slave deployment is upgraded to controller switching architecture

   In-place upgrade with data is possible. For each group of Brokers, stop the primary and secondary Brokers and ensure that the CommitLogs of the primary and secondary are aligned (you can either disable writing to this group of Brokers for a certain period of time before the upgrade or ensure consistency by copying). After upgrading the package, restart it.

   > If the primary and secondary CommitLogs are not aligned, it is necessary to ensure that the primary is online before the secondary is online, otherwise messages may be lost due to data truncation.

2. Upgrade from DLedger mode to Controller switching architecture

   Due to the differences in the format of message data in DLedger mode and Master-Slave mode, there is no in-place upgrade with data. In the case of deploying multiple groups of Brokers, it is possible to disable writing to a group of Brokers for a certain period of time (as long as it is confirmed that all existing messages have been consumed), and then upgrade and deploy the Controller and new Brokers. In this way, the new Brokers will consume messages from the existing Brokers and the existing Brokers will consume messages from the new Brokers until the consumption is balanced, and then the existing Brokers can be decommissioned.

### Upgrade considerations for persistent BrokerID version

The current version supports a new high-availability architecture with persistent BrokerID version. Upgrading from version 5.x to the current version requires the following considerations:

For version 4.x, follow the above procedure to upgrade.

For upgrading from non-persistent BrokerID version in 5.x to persistent BrokerID version, follow the procedure below:

**Upgrade Controller**

1. Stop the old version Controller group.
2. Clear Controller data, i.e., data files located in `~/DLedgerController` by default.
3. Bring up the new version Controller group. 

> During the Controller upgrade process, Broker can still run normally but cannot failover.

**Upgrade Broker**

1. Stop the secondary Broker.
2. Stop the primary Broker.
3. Delete all Epoch files of all Brokers, i.e., `~/store/epochFileCheckpoint` and `~/store/epochFileCheckpoint.bak` by default.
4. Bring up the original primary Broker and wait for it to be elected as master (you can use the `getSyncStateSet` command of admin to observe).
5. Bring up all the original secondary Brokers.

> It is recommended to stop the secondary Broker before stopping the primary Broker and bring up the original primary Broker before the original secondary during the online process. This can ensure the original primary-secondary relationship.
> If you need to change the primary-secondary relationship before and after the upgrade, make sure that the CommitLog of the primary and secondary are aligned when shutting down. Otherwise, data may be truncated and lost.