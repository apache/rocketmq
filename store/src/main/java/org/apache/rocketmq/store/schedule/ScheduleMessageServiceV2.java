
package org.apache.rocketmq.store.schedule;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import io.netty.util.internal.PlatformDependent;

/**
 * unlimit delay schedule message
 */
public class ScheduleMessageServiceV2 extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private long lastOffset = 0;
    private MessageStore writeMessageStore;
    private final DelayTimeService delayTimeService;
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    private volatile long nextTime;
    private final Thread workerThread;
    private final ScheduledExecutorService scheduledExecutorService;

    private final long maxDelayTime;

    public ScheduleMessageServiceV2(final DefaultMessageStore defaultMessageStore, DelayTimeService delayTimeService) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
        this.delayTimeService  = delayTimeService;
        workerThread = Executors.defaultThreadFactory().newThread(new TimerLineTask());
        maxDelayTime = defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds() * 1000;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }
    private void updateOffset(long offset) {
        this.lastOffset = offset;
    }

    public long computeDeliverTimestamp(final long delayTime, final long storeTimestamp) {
        return delayTime + storeTimestamp;
    }

    // 写文件
    public void buildDelayTime(DispatchRequest request) {
        delayTimeService.buildDelayTime(request);
    }

    public void start() {
        // 先加载上一次读取的偏移量
        // 启动异步线程检查任务
        if (started.compareAndSet(false, true)) {
            load();
            final long currentTimeMillis = System.currentTimeMillis();
            if (lastOffset + 1000 < currentTimeMillis) {
                final long firstOffset = delayTimeService.getFirstOffset();
                if (firstOffset != 0) {
                   long beginTimestamp = Math.max(lastOffset + 1000, firstOffset);
                   while(beginTimestamp <= System.currentTimeMillis() + 1000) {
                       dealTimerLine(beginTimestamp);
                       beginTimestamp = beginTimestamp + 1000;
                   }
                }
            }
            workerThread.start();
            // Wait until the startTime is initialized by the worker.
            while (nextTime == 0) {
                try {
                    startTimeInitialized.await();
                } catch (InterruptedException ignore) {
                    // Ignore - it will be ready very soon.
                }
            }

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    cleanFilesPeriodically();
                }
            }, 1000 * 60, 60000, TimeUnit.MILLISECONDS);
        }
    }

    private void cleanFilesPeriodically() {
        delayTimeService.tryDeleteExpireFiles();
    }

    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            delayTimeService.shutdown();
            System.out.println("shutdown...");
            persist();
        }
    }

    public long getMaxDelayTime(){
        return maxDelayTime;
    }

    @Override
    public String encode() {
        return String.valueOf(lastOffset);
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayTimeOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (StringUtils.isNotBlank(jsonString)) {
            lastOffset = Long.valueOf(jsonString);
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return String.valueOf(lastOffset);
    }

    class TimerLineTask implements Runnable {

        @Override
        public void run() {
            // Initializ;
            nextTime =  System.currentTimeMillis() + 1000;

            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    dealTimerLine(nextTime);
                }
                nextTime = nextTime + 1000;
            } while (started.get());
            log.info("TimerLineTask has stop");
        }

        public long waitForNextTick() {

            for (;;) {
                long sleepTimeMs = nextTime - System.currentTimeMillis();

                if (sleepTimeMs <= 0) {
                    if (nextTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return nextTime;
                    }
                }

                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                if (PlatformDependent.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                    if (sleepTimeMs == 0) {
                        sleepTimeMs = 1;
                    }
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (!started.get()) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

    }

    public void dealTimerLine(long currentTimeMillis) {
        final DelayTimeResult result = delayTimeService.queryByTimestamp(currentTimeMillis);
        if (result == null) {
            return;
        }
        boolean syncDeliver = true;
        final List<DelayTimeResult.DelayTimeItem> delayTimeItemList = result.getDelayTimeItemList();
        for (DelayTimeResult.DelayTimeItem delayTimeItem : delayTimeItemList) {
            final MessageExt messageExt = ScheduleMessageServiceV2.this.defaultMessageStore
                .lookMessageByOffset(delayTimeItem.getPhyOffset(), delayTimeItem.getMsgSize());
            if (messageExt == null) {
                continue;
            }
            final MessageExtBrokerInner messageExtBrokerInner = messageTimeup(messageExt);
            final CompletableFuture<PutMessageResult> putMessageResultCompletableFuture =
                writeMessageStore.asyncPutMessage(messageExtBrokerInner);
            try {
                final PutMessageResult putMessageResult = putMessageResultCompletableFuture.get();
                if (!putMessageResult.isOk()) {
                    syncDeliver = false;
                    log.error("syncDeliver writeMessageStore.asyncPutMessage has error, "+ putMessageResult.getPutMessageStatus().name());
                }
            } catch (InterruptedException e) {
                syncDeliver = false;
                log.error("syncDeliver writeMessageStore.asyncPutMessage has error", e);
            } catch (ExecutionException e) {
                syncDeliver = false;
                log.error("syncDeliver writeMessageStore.asyncPutMessage has error", e);
            }
        }
        if (syncDeliver) {
            lastOffset = currentTimeMillis;
        }
    }


    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getStoreTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }
}