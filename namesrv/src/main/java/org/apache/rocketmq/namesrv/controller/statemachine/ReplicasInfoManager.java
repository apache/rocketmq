package org.apache.rocketmq.namesrv.controller.statemachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.rocketmq.namesrv.controller.event.AlterIsrEvent;
import org.apache.rocketmq.namesrv.controller.event.ApplyBrokerIdEvent;
import org.apache.rocketmq.namesrv.controller.event.ElectMasterEvent;
import org.apache.rocketmq.namesrv.controller.event.EventMessage;
import org.apache.rocketmq.namesrv.controller.event.EventType;
import org.apache.rocketmq.namesrv.controller.event.TryToBeMasterEvent;

/**
 * The manager that manages the replicas info for all brokers.
 * We can think of this class as the controller's memory state machine
 * It should be noted that this class is not thread safe,
 * and the upper layer needs to ensure that it can be called sequentially
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:00
 */
public class ReplicasInfoManager {

    private static final Long LEADER_ID = 0L;
    private final Map<String/* brokerName */, BrokerIdInfo> replicaInfoTable;
    private final Map<String/* brokerName */, InSyncReplicasInfo> inSyncReplicasInfoTable;

    public ReplicasInfoManager() {
        this.replicaInfoTable = new HashMap<>();
        this.inSyncReplicasInfoTable = new HashMap<>();
    }

    public void ApplyEvent(final EventMessage event) {
        final EventType type = event.eventType();
        switch (type) {
            case ALTER_ISR_EVENT:
                handleAlterIsr((AlterIsrEvent) event);
                break;
            case APPLY_BROKER_ID_EVENT:
                handleApplyBrokerId((ApplyBrokerIdEvent) event);
                break;
            case ELECT_MASTER_EVENT:
                handleElectMaster((ElectMasterEvent) event);
                break;
            case TRY_TO_BE_MASTER_EVENT:
                handleTryToBeMaster((TryToBeMasterEvent) event);
                break;
            case READ_EVENT:
                break;
            default:
                break;
        }
    }

    private void handleAlterIsr(final AlterIsrEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final int isrEpoch = replicasInfo.getIsrEpoch();
            replicasInfo.setIsrEpoch(isrEpoch + 1);
            replicasInfo.setInSyncReplicas(event.getNewIsr());
        }
    }

    private void handleApplyBrokerId(final ApplyBrokerIdEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            if (!brokerIdTable.containsKey(event.getBrokerAddress())) {
                brokerIdTable.put(event.getBrokerAddress(), event.getNewBrokerId());
            }
        }
    }

    private void handleElectMaster(final ElectMasterEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();

            // Step1, change the origin master to follower
            final String originMaster = replicasInfo.getMasterAddress();
            final long originMasterId = replicasInfo.getMasterOriginId();
            brokerIdTable.put(originMaster, originMasterId);

            // Step2, record new master
            final String newMaster = event.getNewMasterAddress();
            final Long newMasterOriginId = brokerIdTable.get(newMaster);
            brokerIdTable.put(newMaster, LEADER_ID);
            replicasInfo.setMasterAddress(newMaster);
            replicasInfo.setMasterOriginId(newMasterOriginId);
            replicasInfo.setMasterEpoch(replicasInfo.getMasterEpoch() + 1);

            // Step3, record new isr list
            final HashSet<String> isr = new HashSet<>();
            isr.add(newMaster);
            replicasInfo.setInSyncReplicas(isr);
            replicasInfo.setIsrEpoch(replicasInfo.getIsrEpoch() + 1);
        }
    }

    /**
     * Because this method will only be triggered when the first replicas of the broker come online,
     * we can create memory meta information for the broker in this method
     */
    private void handleTryToBeMaster(final TryToBeMasterEvent event) {
        final String brokerName = event.getBrokerName();
        final String clusterName = event.getClusterName();
        final String masterAddress = event.getBrokerAddress();
        if (!isContainsBroker(brokerName)) {
            final BrokerIdInfo brokerInfo = new BrokerIdInfo(clusterName, brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            final InSyncReplicasInfo replicasInfo = new InSyncReplicasInfo(clusterName, brokerName, masterAddress);
            brokerIdTable.put(masterAddress, LEADER_ID);
            this.inSyncReplicasInfoTable.put(brokerName, replicasInfo);
            this.replicaInfoTable.put(brokerName, brokerInfo);
        }
    }

    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.inSyncReplicasInfoTable.containsKey(brokerName);
    }

}
