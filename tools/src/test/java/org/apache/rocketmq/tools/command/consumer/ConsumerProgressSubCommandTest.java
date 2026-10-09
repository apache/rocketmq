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
package org.apache.rocketmq.tools.command.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ConsumerProgressSubCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = NameServerMocker.startByDefaultConf(brokerMocker.listenPort());
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Ignore
    @Test
    public void testExecute() throws SubCommandException {
        ConsumerProgressSubCommand cmd = new ConsumerProgressSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g default-group",
            String.format("-n localhost:%d", nameServerMocker.listenPort())};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startOneBroker() {
        ConsumeStats consumeStats = new ConsumeStats();
        HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName("mockBrokerName");
        messageQueue.setQueueId(1);
        messageQueue.setBrokerName("mockTopicName");

        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setBrokerOffset(1);
        offsetWrapper.setConsumerOffset(1);
        offsetWrapper.setLastTimestamp(System.currentTimeMillis());

        offsetTable.put(messageQueue, offsetWrapper);
        consumeStats.setOffsetTable(offsetTable);
        // start broker
        return ServerResponseMocker.startServer(consumeStats.encode());
    }

    @Test
    public void testCompareToIsAntisymmetricAcrossIntegerMaxValue() {
        GroupConsumeInfo zeroLag = buildGroupConsumeInfo("zero-lag", 0L);
        GroupConsumeInfo overIntLag = buildGroupConsumeInfo("over-int-lag", (long) Integer.MAX_VALUE + 1L);
        GroupConsumeInfo maxLag = buildGroupConsumeInfo("max-lag", Long.MAX_VALUE);

        // A subtraction-based comparator wraps for gaps beyond Integer.MAX_VALUE and would
        // then report both a < b and b < a; assert the antisymmetric contract explicitly.
        Assert.assertTrue(zeroLag.compareTo(overIntLag) > 0);
        Assert.assertTrue(overIntLag.compareTo(zeroLag) < 0);
        Assert.assertTrue(overIntLag.compareTo(maxLag) > 0);
        Assert.assertTrue(maxLag.compareTo(overIntLag) < 0);
        Assert.assertEquals(0, overIntLag.compareTo(buildGroupConsumeInfo("over-int-lag-copy", (long) Integer.MAX_VALUE + 1L)));
        Assert.assertEquals(0, maxLag.compareTo(buildGroupConsumeInfo("max-lag-copy", Long.MAX_VALUE)));
    }

    @Test
    public void testCollectionsSortOrdersGroupsByLagDescending() {
        GroupConsumeInfo zeroLag = buildGroupConsumeInfo("zero-lag", 0L);
        GroupConsumeInfo overIntLag = buildGroupConsumeInfo("over-int-lag", (long) Integer.MAX_VALUE + 1L);
        GroupConsumeInfo maxLag = buildGroupConsumeInfo("max-lag", Long.MAX_VALUE);

        List<GroupConsumeInfo> groups = new ArrayList<>();
        groups.add(zeroLag);
        groups.add(maxLag);
        groups.add(overIntLag);
        Collections.sort(groups);

        Assert.assertEquals(Arrays.asList(maxLag, overIntLag, zeroLag), groups);
    }

    @Test
    public void testSortWithLagValuesSpanningIntAndLongRanges() {
        List<GroupConsumeInfo> groups = new ArrayList<>();
        groups.add(buildGroupConsumeInfo("lag-zero", 0L));
        groups.add(buildGroupConsumeInfo("lag-small", 1024L));
        groups.add(buildGroupConsumeInfo("lag-over-int", (long) Integer.MAX_VALUE + 1L));
        groups.add(buildGroupConsumeInfo("lag-max", Long.MAX_VALUE));

        Collections.sort(groups);

        // Every adjacent pair must be ordered descending, which only holds when the
        // comparison never wraps around for gaps larger than Integer.MAX_VALUE.
        for (int i = 0; i < groups.size() - 1; i++) {
            Assert.assertTrue(groups.get(i).getDiffTotal() > groups.get(i + 1).getDiffTotal());
            Assert.assertTrue(groups.get(i).compareTo(groups.get(i + 1)) < 0);
        }
        Assert.assertEquals("lag-max", groups.get(0).getGroup());
        Assert.assertEquals("lag-zero", groups.get(groups.size() - 1).getGroup());
    }

    @Test
    public void testCompareToPrefersMoreConsumersBeforeLag() {
        GroupConsumeInfo singleConsumer = buildGroupConsumeInfo("single-consumer", Long.MAX_VALUE);
        GroupConsumeInfo twoConsumers = buildGroupConsumeInfo("two-consumers", 0L);
        twoConsumers.setCount(2);

        // The count comparison short-circuits the diffTotal comparison: groups with more
        // live connections are always ordered first, whatever their lag is.
        Assert.assertTrue(singleConsumer.compareTo(twoConsumers) > 0);
        Assert.assertTrue(twoConsumers.compareTo(singleConsumer) < 0);
    }

    private static GroupConsumeInfo buildGroupConsumeInfo(String group, long diffTotal) {
        GroupConsumeInfo info = new GroupConsumeInfo();
        info.setGroup(group);
        // Keep the connection count identical so compareTo falls through to the diffTotal
        // comparison that is under test.
        info.setCount(1);
        info.setDiffTotal(diffTotal);
        return info;
    }
}
