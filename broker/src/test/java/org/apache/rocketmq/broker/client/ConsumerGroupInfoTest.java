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
package org.apache.rocketmq.broker.client;

import java.util.Collections;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Level;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Logger;
import org.apache.rocketmq.logging.ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.rocketmq.logging.ch.qos.logback.core.read.ListAppender;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupInfoTest {
    private final Logger logger = (Logger) LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private Level previousLevel;
    private ConsumerGroupInfo group;

    @Before
    public void setUp() {
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        appender.start();
        logger.addAppender(appender);
        group = new ConsumerGroupInfo("group", ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    }

    @After
    public void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(previousLevel);
    }

    @Test
    public void testSameContentAdvancesVersionWithoutLogging() throws Exception {
        for (String type : new String[] {ExpressionType.TAG, ExpressionType.SQL92}) {
            group.getSubscriptionTable().clear();
            SubscriptionData old = subscription("a > 0", type, 100L);
            assertThat(group.updateSubscription(Collections.singleton(old))).isTrue();
            appender.list.clear();

            SubscriptionData newer = subscription("a > 0", type, 300L);
            assertThat(group.updateSubscription(Collections.singleton(newer))).isFalse();
            assertThat(group.findSubscriptionData("topic")).isSameAs(newer);
            assertThat(old.getSubVersion()).isEqualTo(100L);
            assertThat(appender.list).isEmpty();

            SubscriptionData stale = subscription("a > 1", type, 200L);
            assertThat(group.updateSubscription(Collections.singleton(stale))).isFalse();
            assertThat(group.findSubscriptionData("topic")).isSameAs(newer);
            assertThat(appender.list).isEmpty();
        }
    }

    @Test
    public void testChangedContentStillLogs() throws Exception {
        assertChangeLogged(subscription("tagA", ExpressionType.TAG, 100L),
            subscription("tagB", ExpressionType.TAG, 200L));
        assertChangeLogged(subscription("a > 0", ExpressionType.SQL92, 100L),
            subscription("a > 1", ExpressionType.SQL92, 200L));
        assertChangeLogged(subscription("a > 0", ExpressionType.TAG, 100L),
            subscription("a > 0", ExpressionType.SQL92, 200L));
    }

    @Test
    public void testActiveConsumerStillDoesNotLogContentChanges() throws Exception {
        group.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        group.updateSubscription(Collections.singleton(subscription("tagA", ExpressionType.TAG, 100L)));
        appender.list.clear();

        SubscriptionData newer = subscription("tagB", ExpressionType.TAG, 200L);
        assertThat(group.updateSubscription(Collections.singleton(newer))).isFalse();
        assertThat(group.findSubscriptionData("topic")).isSameAs(newer);
        assertThat(appender.list).isEmpty();
    }

    private void assertChangeLogged(SubscriptionData old, SubscriptionData newer) {
        group.getSubscriptionTable().put("topic", old);
        appender.list.clear();

        assertThat(group.updateSubscription(Collections.singleton(newer))).isFalse();
        assertThat(group.findSubscriptionData("topic")).isSameAs(newer);
        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMessage()).isEqualTo("subscription changed, group: {} OLD: {} NEW: {}");
    }

    private SubscriptionData subscription(String expression, String type, long version) throws Exception {
        SubscriptionData data = FilterAPI.build("topic", expression, type);
        data.setSubVersion(version);
        return data;
    }
}
