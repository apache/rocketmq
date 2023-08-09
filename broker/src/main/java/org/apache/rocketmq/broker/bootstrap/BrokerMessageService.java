/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.bootstrap;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.HookUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.plugin.MessageStoreFactory;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;

public class BrokerMessageService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private volatile boolean isTransactionCheckServiceStart = false;
    private volatile boolean isScheduleServiceStart = false;

    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ConcurrentMap<String, TopicConfig> topicConfigTable;

    private final BrokerController brokerController;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;

    private final ScheduleMessageService scheduleMessageService;
    private TimerCheckpoint timerCheckpoint;
    private final EscapeBridge escapeBridge;

    protected TransactionalMessageCheckService transactionalMessageCheckService;
    protected TransactionalMessageService transactionalMessageService;
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;

    private MessageStore messageStore;
    private TimerMessageStore timerMessageStore;
    private BrokerStats brokerStats;

    public BrokerMessageService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
        this.brokerStatsManager = brokerController.getBrokerStatsManager();
        this.messageArrivingListener = brokerController.getBrokerNettyServer().getMessageArrivingListener();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.topicConfigTable = brokerController.getTopicConfigManager().getTopicConfigTable();

        this.scheduleMessageService = new ScheduleMessageService(brokerController);
        this.escapeBridge = new EscapeBridge(brokerController);
    }

    public boolean init() {
        boolean result = true;
        try {
            DefaultMessageStore defaultMessageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager,
                this.messageArrivingListener, this.brokerConfig, topicConfigTable);

            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(brokerController, defaultMessageStore);
                DLedgerCommitLog dLedgerCommitLog =   (DLedgerCommitLog) defaultMessageStore.getCommitLog();

                dLedgerCommitLog.getdLedgerServer()
                    .getDLedgerLeaderElector()
                    .addRoleChangeHandler(roleChangeHandler);
            }

            this.brokerStats = new BrokerStats(defaultMessageStore);

            // Load store plugin
            MessageStorePluginContext context = new MessageStorePluginContext(
                messageStoreConfig, brokerStatsManager, this.messageArrivingListener, brokerConfig, brokerController.getConfiguration());

            this.messageStore = MessageStoreFactory.build(context, defaultMessageStore);

            this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, brokerController.getConsumerFilterManager()));

            if (messageStoreConfig.isTimerWheelEnable()) {
                this.timerCheckpoint = new TimerCheckpoint(BrokerPathConfigHelper.getTimerCheckPath(messageStoreConfig.getStorePathRootDir()));
                TimerMetrics timerMetrics = new TimerMetrics(BrokerPathConfigHelper.getTimerMetricsPath(messageStoreConfig.getStorePathRootDir()));
                this.timerMessageStore = new TimerMessageStore(messageStore, messageStoreConfig, timerCheckpoint, timerMetrics, brokerStatsManager);
                this.timerMessageStore.registerEscapeBridgeHook(msg -> escapeBridge.putMessage(msg));
                this.messageStore.setTimerMessageStore(this.timerMessageStore);
            }

            if (messageStore != null) {
                registerMessageStoreHook();
                result = this.messageStore.load();
            }

            if (messageStoreConfig.isTimerWheelEnable()) {
                result = result && this.timerMessageStore.load();
            }

            //scheduleMessageService load after messageStore load success
            result = result && this.scheduleMessageService.load();
            initTransaction();

        } catch (IOException e) {
            result = false;
            LOG.error("BrokerController#initialize: unexpected error occurs", e);
        }
        return result;
    }

    private void initTransaction() {
        this.transactionalMessageService = ServiceProvider.loadClass(TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            this.transactionalMessageService = new TransactionalMessageServiceImpl(
                new TransactionalMessageBridge(brokerController, this.getMessageStore()));
            LOG.warn("Load default transaction message hook service: {}",
                TransactionalMessageServiceImpl.class.getSimpleName());
        }
        this.transactionalMessageCheckListener = ServiceProvider.loadClass(
            AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            LOG.warn("Load default discard message hook service: {}",
                DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(brokerController);
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(brokerController);
    }
    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.timerMessageStore != null) {
            this.timerMessageStore.start();
        }

        if (this.escapeBridge != null) {
            this.escapeBridge.start();
        }
    }

    public void shutdown() {
        //it is better to make sure the timerMessageStore shutdown firstly
        if (this.timerMessageStore != null) {
            this.timerMessageStore.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        if (this.scheduleMessageService != null) {
            this.scheduleMessageService.persist();
            this.scheduleMessageService.shutdown();
        }

        if (this.escapeBridge != null) {
            escapeBridge.shutdown();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

    }

    public synchronized void changeTransactionCheckServiceStatus(boolean shouldStart) {
        if (isTransactionCheckServiceStart != shouldStart) {
            LOG.info("TransactionCheckService status changed to {}", shouldStart);
            if (shouldStart) {
                this.transactionalMessageCheckService.start();
            } else {
                this.transactionalMessageCheckService.shutdown(true);
            }
            isTransactionCheckServiceStart = shouldStart;
        }
    }

    public synchronized void changeScheduleServiceStatus(boolean shouldStart) {
        if (isScheduleServiceStart != shouldStart) {
            LOG.info("ScheduleServiceStatus changed to {}", shouldStart);
            if (shouldStart) {
                this.scheduleMessageService.start();
            } else {
                this.scheduleMessageService.stop();
            }
            isScheduleServiceStart = shouldStart;

            if (timerMessageStore != null) {
                timerMessageStore.setShouldRunningDequeue(shouldStart);
            }
        }
    }

    private void registerMessageStoreHook() {
        List<PutMessageHook> putMessageHookList = messageStore.getPutMessageHookList();

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "checkBeforePutMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                return HookUtils.checkBeforePutMessage(brokerController, msg);
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "innerBatchChecker";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.checkInnerBatch(brokerController, msg);
                }
                return null;
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "handleScheduleMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.handleScheduleMessage(brokerController, (MessageExtBrokerInner) msg);
                }
                return null;
            }
        });

        SendMessageBackHook sendMessageBackHook = new SendMessageBackHook() {
            @Override
            public boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr) {
                return HookUtils.sendMessageBack(brokerController, msgList, brokerName, brokerAddr);
            }
        };

        if (messageStore != null) {
            messageStore.setSendMessageBackHook(sendMessageBackHook);
        }
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    public EscapeBridge getEscapeBridge() {
        return escapeBridge;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerMessageStore getTimerMessageStore() {
        return timerMessageStore;
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
        TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
        AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }

    public boolean isTransactionCheckServiceStart() {
        return isTransactionCheckServiceStart;
    }

    public boolean isScheduleServiceStart() {
        return isScheduleServiceStart;
    }

}
