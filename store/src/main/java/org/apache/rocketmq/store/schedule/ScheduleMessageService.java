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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

//定时消息服务
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";  //定时消息主图
    private static final long FIRST_DELAY_TIME = 1000L; //第一次调度延时的时间
    private static final long DELAY_FOR_A_WHILE = 100L; // 每一延时级别调度一次后延迟改时间间隔后在次放入调度池
    private static final long DELAY_FOR_A_PERIOD = 10000L; //发送异常后延时这么长的时间在继续参与调度

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);// 存储延时级别

    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32); //延迟级别的消费 消费进度

    private final Timer timer = new Timer("ScheduleMessageTimerThread", true); //time 执行器

    private final DefaultMessageStore defaultMessageStore; //默认的消息存储

    private int maxDelayLevel;  //最大的消息延时级别

    //方法的调用顺序 构造方法 -- > ≈() -- > start()

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }  //延时级别与消息队列的id对应关心  queeuId = delayLevel - 1 延时级别-1

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    //start 方法根据延时级别不同创建对应的定时任务
    public void start() {

        for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
            Integer level = entry.getKey();  //延时级别
            Long timeDelay = entry.getValue(); //延时的时间戳
            Long offset = this.offsetTable.get(level); //获取消费进度
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay != null) {
                this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
            }
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {
            //start的同事 每隔10s持久化消费进度
            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }

    public void shutdown() {
        this.timer.cancel();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        //主要完成延时消费队列消费进度的加载 与delayLeavelTable的数据构造 文件delayOffset.json 文件
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
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

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    //装载delayLevelTable 延时级别
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);                 //毫秒
        timeUnitTable.put("m", 1000L * 60);            //分钟
        timeUnitTable.put("h", 1000L * 60 * 60);       //小时
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);  // 天数

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        //具体的延时队列处理逻辑
        @Override
        public void run() {
            try {
                this.executeOnTimeup();
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        //具体定时任务的执行逻辑
        public void executeOnTimeup() {

            //根据队列Id与主题信息查找消费队列
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,//延时队列的topic
                    delayLevel2QueueId(delayLevel));//获取到ConsumeQueue

            long failScheduleOffset = offset;

            if (cq != null) {  //ConsumeQueue
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset); //根据offset 获取byteBuffer  MappedFile文件
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset; //初始偏移量
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();  //consumeQueue 扩展
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {  //ConsumeQueue 的每个单元的长度是20
                            long offsetPy = bufferCQ.getByteBuffer().getLong(); //消息的物理偏移量
                            int sizePy = bufferCQ.getByteBuffer().getInt(); //消息的长度
                            long tagsCode = bufferCQ.getByteBuffer().getLong(); //获取tag的hashCode

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

                            long now = System.currentTimeMillis(); //当前的时间戳
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            /**
                             * 初始偏移量 offset = 20   nextOffset = 20 + （1 / 20）
                             *                        nextOffset = 20 + （2 / 20）
                             *
                             *                        nextOffset = 20 + （21 / 20）一份单元的长度是20
                             */
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now; //

                            if (countdown <= 0) {
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy); //查询消息

                                if (msgExt != null) {  //查询到消息
                                    try {

                                        //根据msgext重新构建消息对象
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.defaultMessageStore
                                                .putMessage(msgInner); //重新投递消息 存入commitLog并转发到主题对应的消息队列上去

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        //更新消息进度 存入到下个定时任无中去
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
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
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }

    public static void main(String [] args){
        System.out.println(1/20);
    }
}
