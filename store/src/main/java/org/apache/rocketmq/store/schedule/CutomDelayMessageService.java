package org.apache.rocketmq.store.schedule;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName CutomDelayMessageService
 * @Version 1.0
 * @Author dragonboy
 * @Date 2021/9/6 14:12
 * @Description
 **/
public class CutomDelayMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "DRAGON_TOPIC_XXX";

    private static final Pattern pattern = Pattern.compile("([0-9]+d)*([0-9]+h)*([0-9]+m)*([0-9]+s)*");

    private static final HashMap<String, Long> timeUnitTable = new HashMap<String, Long>() {{
        this.put("s", 1000L);
        this.put("m", 1000L * 60);
        this.put("h", 1000L * 60 * 60);
        this.put("d", 1000L * 60 * 60 * 24);
    }};

    //最大限度的处理层级
    public static int MAX_LIMIT_LEVEL = 0;
    private static final long FIRST_DELAY_TIME = 1L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10L;

    private ScheduledThreadPoolExecutor poolExecutor;

    private final DefaultMessageStore defaultMessageStore;

    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(256);

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(256);

    private final ConcurrentHashMap<String, Integer> levelMapper = new ConcurrentHashMap<>(256);

    private MessageStore writeMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public CutomDelayMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public int getMaxDelayLevel() {
        return MAX_LIMIT_LEVEL;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getSpcifyDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.poolExecutor)
                this.poolExecutor.shutdown();
        }

    }

    private boolean parseDelayLevel() {
        Integer index = 0;
        try {
            // second
            for (int i = 1; i < 60; i++) {
                delayLevelTable.put(index, i * timeUnitTable.get("s"));
                levelMapper.put(String.valueOf(i + "s"), index++);
            }
            // min
            for (int i = 0; i <= 60; i++) {
                delayLevelTable.put(index, i * timeUnitTable.get("m"));
                levelMapper.put(String.valueOf(i + "m"), index++);
            }
            //hour
            for (int i = 0; i < 24; i++) {
                delayLevelTable.put(index, i * timeUnitTable.get("h"));
                levelMapper.put(String.valueOf(i + "h"), index++);
            }
            //day max time 7
            for (int i = 1; i <= 7; i++) {
                delayLevelTable.put(index, i * timeUnitTable.get("d"));
                levelMapper.put(String.valueOf(i + "d"), index++);
            }
            MAX_LIMIT_LEVEL = index;
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            return false;
        }
        return true;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.poolExecutor = new ScheduledThreadPoolExecutor(delayLevelTable.size() + 1, new DefaultThreadFactory(
                    "DragonMessageTimerSchedule", true));
            for (Map.Entry<Integer /* level */, Long/* delay timeMillis */> entry :
                    delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }
                if (timeDelay != null) {
                    this.poolExecutor.schedule(new DragonScheduleTimeTask(level, offset),
                            FIRST_DELAY_TIME, TimeUnit.SECONDS);
                }
            }
            this.poolExecutor.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            CutomDelayMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    /**
     * 开始状态
     *
     * @return
     */
    public boolean isStarted() {
        return started.get();
    }

    /**
     * 更新偏移量
     *
     * @param delayLevel
     * @param offset
     */
    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 对消息时间进行处理
     *
     * @param delayLevel
     * @param storeTimestamp
     * @return
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp;
    }

    public void dealSpecifyDelayTime(final MessageExtBrokerInner msg) {
        final String specifyDelayTime = msg.getProperties().get(MessageConst.PROPERTY_SPECIFY_DELAY_TIME);
        if (StringUtils.isBlank(specifyDelayTime)) {
            log.warn("msg properties incloud PROPERTY_SPECIFY_DELAY_TIME,but no found value" + msg.getTopic() + " tags: " + msg.getTags()
                    + " client address: " + msg.getBornHostString());
            return;
        }
        String topic = msg.getTopic();
        int queueId = msg.getQueueId();
        final Matcher matcher = pattern.matcher(specifyDelayTime);
        if (!matcher.find()) {
            return;
        }
        final String d = StringUtils.isNotBlank(matcher.group(1)) ? matcher.group(1) : "";
        final String h = StringUtils.isNoneBlank(matcher.group(2)) ? matcher.group(2) : "";
        final String m = StringUtils.isNoneBlank(matcher.group(3)) ? matcher.group(3) : "";
        final String s = StringUtils.isNoneBlank(matcher.group(4)) ? matcher.group(4) : "";
        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_TIME);
        if (StringUtils.isNotBlank(d)) {
            queueId = levelMapper.getOrDefault(d, -1);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_TIME, h + m + s);
        } else if (StringUtils.isNotBlank(h)) {
            final long l = Long.parseLong(h.substring(0, h.length() - 1));
            if (l >= 24) {
                log.error("minute time more than 60");
                return;
            }
            queueId = levelMapper.getOrDefault(h, -1);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_TIME, m + s);
        } else if (StringUtils.isNotBlank(m)) {
            final long l = Long.parseLong(m.substring(0, m.length() - 1));
            if (l >= 60) {
                log.error("minute time more than 60");
                return;
            }
            queueId = levelMapper.getOrDefault(m, -1);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_TIME, s);
        } else if (StringUtils.isNotBlank(s)) {
            final long l = Long.parseLong(s.substring(0, s.length() - 1));
            if (l >= 60) {
                log.error("second time more than 60");
                return;
            }
            queueId = levelMapper.getOrDefault(s, -1);
        }
        if (queueId < 0) {
            log.info("specify delay time less than 1 " + msg.getTopic() + " tags: " + msg.getTags());
            return;
        }
        if (queueId > CutomDelayMessageService.MAX_LIMIT_LEVEL) {
            queueId = CutomDelayMessageService.MAX_LIMIT_LEVEL;
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_LEVEL, String.valueOf(queueId));
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_SPECIFY_DELAY_TAG, "1");

        topic = CutomDelayMessageService.SCHEDULE_TOPIC;
        // Backup real topic, queueId
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        msg.setTopic(topic);
        msg.setQueueId(queueId);
        msg.setTagsCode(System.currentTimeMillis());
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = next.getKey();
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.dragonMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }


    class DragonScheduleTimeTask extends TimerTask {

        private final int delayLevel;
        private final long offset;

        public DragonScheduleTimeTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnDragonTime();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("dragonScheduleTimeTask, executeOnTimeup exception", e);
                CutomDelayMessageService.this.poolExecutor.schedule(new DragonScheduleTimeTask(
                        this.delayLevel, this.offset), DELAY_FOR_A_PERIOD, TimeUnit.SECONDS);
            }
        }

        private void executeOnDragonTime() {
            ConsumeQueue cq = CutomDelayMessageService.this.defaultMessageStore
                    .findConsumeQueue(SCHEDULE_TOPIC, delayLevel);

            long failScheduleOffset = offset;

            if (cq != null) {
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();
                            //获取 存入时间 得到 最终触发时间
                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                            tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;
                            if (countdown <= 0) {
                                //找到满足条件的数据
                                MessageExt msgExt =
                                        CutomDelayMessageService.this.defaultMessageStore.lookMessageByOffset(
                                                offsetPy, sizePy);
                                if (msgExt != null) {
                                    MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                    if (MixAll.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                msgInner.getTopic(), msgInner);
                                        continue;
                                    }
                                    final String isDragon =
                                            msgInner.getProperties().get(MessageConst.PROPERTY_SPECIFY_DELAY_TIME);
                                    if (StringUtils.isBlank(isDragon)) {
                                        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_SPECIFY_DELAY_TAG);
                                    }
                                    PutMessageResult putMessageResult =
                                            CutomDelayMessageService.this.writeMessageStore
                                                    .putMessage(msgInner);
                                    if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                        continue;
                                    } else {
                                        // XXX: warn and notify me
                                        log.error(
                                                "DragonScheduleTimeTask, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                        CutomDelayMessageService.this.poolExecutor.schedule(
                                                new DragonScheduleTimeTask(this.delayLevel,
                                                        nextOffset), DELAY_FOR_A_PERIOD, TimeUnit.SECONDS);
                                        CutomDelayMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                        return;
                                    }
                                }
                            } else {
                                //这样可以在准点的时候启动
                                CutomDelayMessageService.this.poolExecutor.schedule(
                                        new DragonScheduleTimeTask(this.delayLevel, nextOffset),
                                        countdown, TimeUnit.MILLISECONDS);
                                CutomDelayMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        }
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        CutomDelayMessageService.this.poolExecutor.schedule(new DragonScheduleTimeTask(
                                this.delayLevel, nextOffset), DELAY_FOR_A_WHILE, TimeUnit.MILLISECONDS);
                        CutomDelayMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                }
            }
            CutomDelayMessageService.this.poolExecutor.schedule(new DragonScheduleTimeTask(this.delayLevel,
                    failScheduleOffset), DELAY_FOR_A_WHILE, TimeUnit.MILLISECONDS);
        }

        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + CutomDelayMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        private boolean resetQueue(final MessageExtBrokerInner msgInner, final String property) {
            final String time = msgInner.getProperties().get(property);
            if (StringUtils.isBlank(time)) {
                return false;
            }
            int queueId = levelMapper.getOrDefault(time, -1);
            if (queueId != -1) {
                msgInner.setQueueId(queueId);
                msgInner.setTopic(CutomDelayMessageService.SCHEDULE_TOPIC);
                MessageAccessor.clearProperty(msgInner, property);
                //重新插入
                CutomDelayMessageService.this.writeMessageStore.putMessage(msgInner);
                return true;
            }
            return false;
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
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
