package org.apache.rocketmq.broker.dleger;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.dleger.DLegerLeaderElector;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;

public class DLegerRoleChangeHandler implements DLegerLeaderElector.RoleChangeHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private BrokerController brokerController;
    private DefaultMessageStore messageStore;
    public DLegerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
    }

    @Override public void handle(long term, MemberState.Role role) {
        try {
            log.info("Begin handling lastRole change term={} lastRole={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
            switch (role) {
                case CANDIDATE:
                    if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                        brokerController.changeToSlave();
                    }
                    break;
                case FOLLOWER:
                    brokerController.changeToSlave();
                    break;
                case LEADER:
                    while (messageStore.dispatchBehindBytes() != 0) {
                        Thread.sleep(100);
                    }
                    messageStore.recoverTopicQueueTable();
                    brokerController.changeToMaster(BrokerRole.SYNC_MASTER);
                    break;
                default:
                    break;
            }
            log.info("Finish handling lastRole change term={} lastRole={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
        } catch (Throwable t) {
            log.info("Failed handling lastRole change term={} lastRole={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), t);
        }
    }
}
