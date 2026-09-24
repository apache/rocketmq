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

    @Test
    public void testCompareToOrdersByOnlineClientCountFirst() {
        GroupConsumeInfo moreClients = newGroupConsumeInfo("moreClients", 2, 0L);
        GroupConsumeInfo fewerClients = newGroupConsumeInfo("fewerClients", 1, Long.MAX_VALUE);

        // The number of online clients wins over the lag, so the group with more clients is
        // printed first no matter how large the lag of the other group is.
        Assert.assertTrue(moreClients.compareTo(fewerClients) < 0);
        Assert.assertTrue(fewerClients.compareTo(moreClients) > 0);
    }

    @Test
    public void testCompareToOrdersByDiffTotalDescending() {
        GroupConsumeInfo highLag = newGroupConsumeInfo("highLag", 1, 500L);
        GroupConsumeInfo lowLag = newGroupConsumeInfo("lowLag", 1, 100L);
        GroupConsumeInfo sameLag = newGroupConsumeInfo("sameLag", 1, 500L);

        // consumerProgress lists the groups with the largest lag first.
        Assert.assertTrue(highLag.compareTo(lowLag) < 0);
        Assert.assertTrue(lowLag.compareTo(highLag) > 0);
        Assert.assertEquals(0, highLag.compareTo(sameLag));
    }

    @Test
    public void testCompareToDiffTotalBeyondIntRange() {
        // A lag counter is a long value. Casting (o.diffTotal - diffTotal) to an int used to
        // wrap around once the difference exceeded Integer.MAX_VALUE and reversed the order.
        GroupConsumeInfo hugeLag = newGroupConsumeInfo("hugeLag", 1, Long.MAX_VALUE);
        GroupConsumeInfo noLag = newGroupConsumeInfo("noLag", 1, 0L);

        Assert.assertTrue(hugeLag.compareTo(noLag) < 0);
        Assert.assertTrue(noLag.compareTo(hugeLag) > 0);
    }

    @Test
    public void testCompareToAntisymmetricForLargeDiffTotals() {
        long[] diffTotals = new long[] {0L, 1L, Integer.MAX_VALUE + 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE};
        for (long left : diffTotals) {
            for (long right : diffTotals) {
                int forward = newGroupConsumeInfo("left", 1, left)
                    .compareTo(newGroupConsumeInfo("right", 1, right));
                int backward = newGroupConsumeInfo("right", 1, right)
                    .compareTo(newGroupConsumeInfo("left", 1, left));
                // A comparator that overflows can report "left < right" and "right < left" at
                // the same time, which breaks the contract the sort algorithm relies on.
                Assert.assertEquals(Integer.signum(forward), -Integer.signum(backward));
            }
        }
    }

    @Test
    public void testSortGroupsByDiffTotalBeyondIntRange() {
        List<GroupConsumeInfo> groups = new ArrayList<>();
        groups.add(newGroupConsumeInfo("zero", 1, 0L));
        groups.add(newGroupConsumeInfo("max", 1, Long.MAX_VALUE));
        groups.add(newGroupConsumeInfo("intMax", 1, Integer.MAX_VALUE + 7L));
        groups.add(newGroupConsumeInfo("small", 1, 3L));

        Collections.sort(groups);

        Assert.assertEquals(Arrays.asList("max", "intMax", "small", "zero"), groupNames(groups));
    }

    @Test
    public void testSortGroupsWithMixedClientCountsAndLargeDiffTotals() {
        List<GroupConsumeInfo> groups = new ArrayList<>();
        groups.add(newGroupConsumeInfo("oneClientHugeLag", 1, Long.MAX_VALUE));
        groups.add(newGroupConsumeInfo("twoClientsNoLag", 2, 0L));
        groups.add(newGroupConsumeInfo("oneClientNoLag", 1, 0L));

        Collections.sort(groups);

        Assert.assertEquals(Arrays.asList("twoClientsNoLag", "oneClientHugeLag", "oneClientNoLag"),
            groupNames(groups));
    }

    private static GroupConsumeInfo newGroupConsumeInfo(String group, int count, long diffTotal) {
        GroupConsumeInfo info = new GroupConsumeInfo();
        info.setGroup(group);
        info.setCount(count);
        info.setDiffTotal(diffTotal);
        return info;
    }

    private static List<String> groupNames(List<GroupConsumeInfo> groups) {
        List<String> names = new ArrayList<>(groups.size());
        for (GroupConsumeInfo group : groups) {
            names.add(group.getGroup());
        }
        return names;
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
}
