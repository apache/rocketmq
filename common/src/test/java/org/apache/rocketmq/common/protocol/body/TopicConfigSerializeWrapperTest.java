package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.TopicConfig;
import org.junit.Assert;
import org.junit.Test;

public class TopicConfigSerializeWrapperTest {

    String topicName = "xxxx";

    @Test
    public void testEncode(){
        TopicConfig config = new TopicConfig(topicName);
        Assert.assertNotNull(TopicConfigSerializeWrapper.encode(config));
    }

    @Test
    public void testDecode(){
        String json = "{\"order\":false,\"perm\":6,\"readQueueNums\":16,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"xxxx\",\"topicSysFlag\":0,\"writeQueueNums\":16}";
        TopicConfig config = TopicConfigSerializeWrapper.decode(json.getBytes(), TopicConfig.class);
        Assert.assertNotNull(config);
        Assert.assertEquals(topicName, config.getTopicName());
    }


}
