package com.alibaba.rocketmq.namesrv.topic;

import static com.alibaba.rocketmq.namesrv.DataUtils.create;
import static com.alibaba.rocketmq.namesrv.DataUtils.createAddr;
import static com.alibaba.rocketmq.namesrv.DataUtils.createExpect;
import static com.alibaba.rocketmq.namesrv.DataUtils.createQueueData;
import static com.alibaba.rocketmq.namesrv.DataUtils.createTopic;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.MERGE_INVALID;
import static com.alibaba.rocketmq.namesrv.common.MergeResult.SYS_ERROR;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.namesrv.DataUtils;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * @auther lansheng.zj@taobao.com
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("com.alibaba.rocketmq.common.MixAll")
@PrepareForTest({ RemotingHelper.class })
public class DefaultTopicRuntimeDataManagerTest {

    private DefaultTopicRuntimeDataManager dataManager;


    @Before
    public void init() {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        namesrvConfig.setNamesrvAddr("10.232.133.1:9876;10.232.133.2:9876;");
        dataManager = new DefaultTopicRuntimeDataManager(namesrvConfig);
        dataManager.init();
    }


    @Test
    public void testMerge() throws Exception {
        TopicRuntimeData expected = createExpect();
        int code = dataManager.merge(expected);
        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(MERGE_INVALID != code);
        Assert.assertTrue(SYS_ERROR != code);
        Assert.assertTrue(topicRuntimeData.equals(expected));
    }


    @Test
    public void testInit() throws Exception {
        TopicRuntimeData expected = createExpect();

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        namesrvConfig.setNamesrvAddr("10.232.133.1:9876;10.232.133.2:9876;");
        DefaultTopicRuntimeDataManager dataManager = new DefaultTopicRuntimeDataManager(namesrvConfig);
        boolean success = dataManager.init();

        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(success);
        Assert.assertTrue(topicRuntimeData.equals(expected));
    }


    @Test
    public void testGetRouteInfoByTopic() throws Exception {
        String topic = "topic-1";
        DataUtils.hackField(dataManager, create(), "topicData");
        RemotingCommand response = dataManager.getRouteInfoByTopic(topic);
        Assert.assertTrue(response.getCode() == ResponseCode.SUCCESS_VALUE);

        TopicRouteData topicRouteData = TopicRouteData.decode(response.getBody(), TopicRouteData.class);
        Assert.assertTrue("topic.num.topic-1=105:4;106:4".equals(topicRouteData.getOrderTopicConf()));
    }


    @Test
    public void testGetTopicRuntimeData() throws Exception {
        DataUtils.hackField(dataManager, DataUtils.createExpect(), "topicData");
        RemotingCommand response = dataManager.getTopicRuntimeData();
        Assert.assertTrue(response.getCode() == SUCCESS_VALUE);

        TopicRuntimeData data = TopicRuntimeData.decode(response.getBody());
        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(topicRuntimeData.equals(data));
    }


    @Test
    public void testRegisterBroker() throws Exception {
        RemotingCommand mockResponse = RemotingCommand.createResponseCommand(null);
        mockResponse.setCode(SUCCESS_VALUE);
        PowerMock.mockStatic(RemotingHelper.class);
        EasyMock
            .expect(
                RemotingHelper.invokeSync(anyObject(String.class), anyObject(RemotingCommand.class), anyLong()))
            .andReturn(mockResponse).anyTimes();
        PowerMock.replay(RemotingHelper.class);

        String addr = createAddr();
        RemotingCommand response = dataManager.registerBroker(addr);

        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(response.getCode() == SUCCESS_VALUE);
        Assert.assertTrue(topicRuntimeData.getBrokerList().contains(addr));
    }


    @Test
    public void testRegisterOrderTopic() throws Exception {
        RemotingCommand mockResponse = RemotingCommand.createResponseCommand(null);
        mockResponse.setCode(SUCCESS_VALUE);
        PowerMock.mockStatic(RemotingHelper.class);
        EasyMock
            .expect(
                RemotingHelper.invokeSync(anyObject(String.class), anyObject(RemotingCommand.class), anyLong()))
            .andReturn(mockResponse).anyTimes();
        PowerMock.replay(RemotingHelper.class);
        RemotingCommand response = dataManager.registerOrderTopic("topic-100", "105:4;106:4");

        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(response.getCode() == SUCCESS_VALUE);
        Assert.assertTrue("topic.num.topic-100=105:4;106:4".equals(topicRuntimeData
            .getOrderConfByTopic("topic-100")));
    }


    @Test
    public void testMergeQueueData() {
        boolean success = dataManager.mergeBrokerData("broker-100", 0L, createAddr());
        Assert.assertTrue(success);
    }


    @Test
    public void testMergeBrokerData() throws Exception {
        String topic = createTopic();
        QueueData queueData = createQueueData();
        Map<String, QueueData> map = new HashMap<String, QueueData>();
        map.put(topic, queueData);
        boolean success = dataManager.mergeQueueData(map);

        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(topicRuntimeData.getTopicBrokers().get(topic).contains(queueData));
        Assert.assertTrue(success);

        success = dataManager.mergeQueueData(map);
        Assert.assertTrue(topicRuntimeData.getTopicBrokers().get(topic).contains(queueData));
        Assert.assertTrue(success);
    }


    @Test
    public void testDoUnRegisterBroker() throws Exception {
        String brokerName = "broker-1";
        TopicRuntimeData data = DataUtils.create();
        DataUtils.hackField(dataManager, data, "topicData");
        boolean ret = dataManager.doUnRegisterBroker(brokerName);
        Assert.assertTrue(ret);
    }


    @Test
    public void testDoUnRegisterBrokerOne() throws Exception {
        String brokerName = "broker-1";
        TopicRuntimeData data = DataUtils.createOne();
        DataUtils.hackField(dataManager, data, "topicData");
        boolean ret = dataManager.doUnRegisterBroker(brokerName);
        Assert.assertTrue(ret);
    }

}
