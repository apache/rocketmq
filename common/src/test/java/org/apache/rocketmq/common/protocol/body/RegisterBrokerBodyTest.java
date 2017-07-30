package org.apache.rocketmq.common.protocol.body;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.TopicConfig;
import org.junit.Assert;
import org.junit.Test;

public class RegisterBrokerBodyTest {

    @Test
    public void encode() throws Exception {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        List<String> filterServerList = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            filterServerList.add("localhost:10911");
        }
        registerBrokerBody.setFilterServerList(filterServerList);

        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();

        ConcurrentMap<String, TopicConfig> topicConfigs = wrapper.getTopicConfigTable();

        NumberFormat numberFormat = new DecimalFormat("0000000000");
        List<String> topics = new ArrayList<String>();
        for (int i = 0; i < 1024; i++) {
            TopicConfig topicConfig = new TopicConfig("Topic" + numberFormat.format(i));
            topicConfigs.put(topicConfig.getTopicName(), topicConfig);
            topics.add(topicConfig.getTopicName());
        }
        registerBrokerBody.setTopicConfigSerializeWrapper(wrapper);

        byte[] compressed = registerBrokerBody.encode(true);

        RegisterBrokerBody registerBrokerBodyBackUp = RegisterBrokerBody.decode(compressed, true);
        ConcurrentMap<String, TopicConfig> backupMap = registerBrokerBodyBackUp.getTopicConfigSerializeWrapper().getTopicConfigTable();

        List<String> backupTopics = new ArrayList<String>();
        for (ConcurrentMap.Entry<String, TopicConfig> next : backupMap.entrySet()) {
            backupTopics.add(next.getKey());
        }
        Collections.sort(topics);
        Collections.sort(backupTopics);
        Assert.assertEquals(topics, backupTopics);
    }
}