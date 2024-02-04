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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.offset;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.awaitility.Awaitility.await;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OffsetResetIT extends BaseConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetIT.class);

    private RMQNormalListener listener = null;
    private RMQNormalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private DefaultMQAdminExt defaultMQAdminExt = null;
    private String topic = null;

    @Before
    public void init() throws MQClientException {
        topic = initTopic();
        LOGGER.info(String.format("use topic: %s;", topic));

        for (BrokerController controller : brokerControllerList) {
            controller.getBrokerConfig().setLongPollingEnable(false);
            controller.getBrokerConfig().setShortPollingTimeMills(500);
            controller.getBrokerConfig().setUseServerSideResetOffset(true);
        }

        listener = new RMQNormalListener();
        producer = getProducer(NAMESRV_ADDR, topic);
        consumer = getConsumer(NAMESRV_ADDR, topic, "*", listener);

        defaultMQAdminExt = BaseConf.getAdmin(NAMESRV_ADDR);
        defaultMQAdminExt.start();
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testEncodeOffsetHeader() {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(consumer.getConsumerGroup());
        requestHeader.setTimestamp(System.currentTimeMillis());
        requestHeader.setForce(false);
        RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
    }

    /**
     * use mq admin tool to query remote offset
     */
    private long getConsumerLag(String topic, String group) throws Exception {
        long consumerLag = 0L;
        for (BrokerController controller : brokerControllerList) {
            ConsumeStats consumeStats = defaultMQAdminExt.getDefaultMQAdminExtImpl()
                .getMqClientInstance().getMQClientAPIImpl()
                .getConsumeStats(controller.getBrokerAddr(), group, topic, 3000);
            Map<MessageQueue, OffsetWrapper> offsetTable = consumeStats.getOffsetTable();

            for (Map.Entry<MessageQueue, OffsetWrapper> entry : offsetTable.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                OffsetWrapper offsetWrapper = entry.getValue();

                Assert.assertEquals(messageQueue.getBrokerName(), controller.getBrokerConfig().getBrokerName());
                long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, messageQueue.getQueueId());
                long consumerOffset = controller.getConsumerOffsetManager().queryOffset(
                    consumer.getConsumerGroup(), topic, messageQueue.getQueueId());
                Assert.assertEquals(brokerOffset, offsetWrapper.getBrokerOffset());
                Assert.assertEquals(consumerOffset, offsetWrapper.getConsumerOffset());

                consumerLag += brokerOffset - consumerOffset;
            }
        }
        return consumerLag;
    }

    @Test
    public void testResetOffsetSingleQueue() throws Exception {
        int msgSize = 100;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg messageQueueMsg = new MessageQueueMsg(mqs, msgSize);

        producer.send(messageQueueMsg.getMsgsWithMQ());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(3)).until(
            () -> 0L == this.getConsumerLag(topic, consumer.getConsumerGroup()));

        for (BrokerController controller : brokerControllerList) {
            defaultMQAdminExt.resetOffsetByQueueId(controller.getBrokerAddr(),
                consumer.getConsumerGroup(), consumer.getTopic(), 3, 0);
        }

        int hasConsumeBefore = listener.getMsgIndex().get();
        int expectAfterReset = brokerControllerList.size() * msgSize;
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(3)).until(() -> {
            long receive = listener.getMsgIndex().get();
            long expect = hasConsumeBefore + expectAfterReset;
            return receive >= expect;
        });
    }

    @Test
    public void testResetOffsetTotal() throws Exception {
        int msgSize = 100;
        long start = System.currentTimeMillis();
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg messageQueueMsg = new MessageQueueMsg(mqs, msgSize);

        producer.send(messageQueueMsg.getMsgsWithMQ());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(3)).until(
            () -> 0L == this.getConsumerLag(topic, consumer.getConsumerGroup()));

        for (BrokerController controller : brokerControllerList) {
            defaultMQAdminExt.getDefaultMQAdminExtImpl().getMqClientInstance().getMQClientAPIImpl()
                .invokeBrokerToResetOffset(controller.getBrokerAddr(),
                    consumer.getTopic(), consumer.getConsumerGroup(), start, true, 3 * 1000);
        }

        int hasConsumeBefore = listener.getMsgIndex().get();
        int expectAfterReset = mqs.size() * msgSize;
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(3)).until(() -> {
            long receive = listener.getMsgIndex().get();
            long expect = hasConsumeBefore + expectAfterReset;
            return receive >= expect;
        });
    }

    @Test
    public void testPullOffsetTotal() throws Exception {
        int msgSize = 100;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg messageQueueMsg = new MessageQueueMsg(mqs, msgSize);

        producer.send(messageQueueMsg.getMsgsWithMQ());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(3)).until(
            () -> 0L == this.getConsumerLag(topic, consumer.getConsumerGroup()));

        long expectInflight = 0L;
        for (BrokerController controller : brokerControllerList) {
            ConsumeStats consumeStats = defaultMQAdminExt.getDefaultMQAdminExtImpl().getMqClientInstance()
                .getMQClientAPIImpl().getConsumeStats(controller.getBrokerAddr(),
                    consumer.getConsumerGroup(), consumer.getTopic(), 3 * 1000);
            expectInflight += consumeStats.computeInflightTotalDiff();
        }
        Assert.assertEquals(0L, expectInflight);
    }
}
