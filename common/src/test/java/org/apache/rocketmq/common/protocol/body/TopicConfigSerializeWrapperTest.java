package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicConfigSerializeWrapperTest {

    @Test
    public void testFromJson(){
        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();

        ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
        TopicConfig topicConfig = new TopicConfig("topic-xxx");
        topicConfigs.put("config", topicConfig);
        wrapper.setTopicConfigTable(topicConfigs);

        DataVersion dataVersion = new DataVersion();
        dataVersion.nextVersion();
        wrapper.setDataVersion(dataVersion);

        String json = RemotingSerializable.toJson(wrapper, false);

        TopicConfigSerializeWrapper fromJsonWrapper =
                RemotingSerializable.fromJson(json, TopicConfigSerializeWrapper.class);

        Assert.assertEquals("topic-xxx", fromJsonWrapper.getTopicConfigTable().get("config").getTopicName());
    }

}
