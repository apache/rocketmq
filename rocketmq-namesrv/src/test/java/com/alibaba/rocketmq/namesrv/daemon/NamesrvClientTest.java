package com.alibaba.rocketmq.namesrv.daemon;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetTopicResponseHeader;
import com.alibaba.rocketmq.namesrv.DataUtils;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.namesrv.topic.DefaultTopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * @auther lansheng.zj@taobao.com
 */
public class NamesrvClientTest {

    private RemotingClient remotingClient;


    @Before
    public void init() {
        remotingClient = EasyMock.createMock(RemotingClient.class);
    }


    @Test
    public void testGroupCommit() throws Exception {
        Map<String, TopicConfig> topicConfigTable = create();
        String content = encode(topicConfigTable);
        RemotingCommand response1 = RemotingCommand.createResponseCommand(GetTopicResponseHeader.class);
        response1.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
        
        GetTopicResponseHeader getTopicResponseHeader=(GetTopicResponseHeader)response1.getCustomHeader();
        getTopicResponseHeader.setVersion("version");
        getTopicResponseHeader.setBrokerName("broker-019");
        getTopicResponseHeader.setBrokerId(0);
        getTopicResponseHeader.setCluster("cluster");
        response1.setCode(ResponseCode.SUCCESS_VALUE);
        response1.setRemark(null);

        RemotingCommand response2 = RemotingCommand.createResponseCommand(GetTopicResponseHeader.class);
        response2.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
        
        GetTopicResponseHeader getTopicResponseHeader2=(GetTopicResponseHeader)response2.getCustomHeader();
        getTopicResponseHeader2.setVersion("version");
        getTopicResponseHeader2.setBrokerName("broker-019");
        getTopicResponseHeader2.setBrokerId(0);
        getTopicResponseHeader2.setCluster("cluster");
        response2.setCode(ResponseCode.SUCCESS_VALUE);
        response2.setRemark(null);

        EasyMock
            .expect(
                remotingClient.invokeSync(eq("12.12.13.4:8123"), anyObject(RemotingCommand.class),
                    anyLong())).andReturn(response1).anyTimes();
        EasyMock
            .expect(
                remotingClient.invokeSync(eq("12.12.13.89:8123"), anyObject(RemotingCommand.class),
                    anyLong())).andReturn(response2).anyTimes();
        EasyMock.replay(remotingClient);

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        namesrvConfig.setNamesrvAddr("120.21.21.12:9876;120.21.21.11:9876;");
        List<String> brokerList = new ArrayList<String>();
        brokerList.add("12.12.13.4:8123");
        brokerList.add("12.12.13.89:8123");
        DefaultTopicRuntimeDataManager dataManager = new DefaultTopicRuntimeDataManager(namesrvConfig);
        TopicRuntimeData topicRuntimeData = DataUtils.getField(dataManager, TopicRuntimeData.class, "topicData");

        topicRuntimeData.setBrokerList(brokerList);

        NettyClientConfig nettyClientConfig = new NettyClientConfig();

        NamesrvClient namesrvClient = new NamesrvClient(namesrvConfig, nettyClientConfig, dataManager);
        DataUtils.hackField(namesrvClient, remotingClient, "remotingClient");

        FutureGroup<Boolean> futureGroup = namesrvClient.groupCommit();
        Result result = namesrvClient.pullSuccess(futureGroup);

        Assert.assertTrue(result.isSuccess());
        Assert.assertTrue("12.12.13.4:8123".equals(topicRuntimeData.getBrokers().get("broker-019")
            .getBrokerAddrs().get(0L)));
    }


    private Map<String, TopicConfig> create() {
        Map<String, TopicConfig> topicConfigTable = new HashMap<String, TopicConfig>(1024);
        topicConfigTable.put("topic-909", new TopicConfig("topic-909", 4, 4, 4));
        topicConfigTable.put("topic-910", new TopicConfig("topic-910", 4, 4, 4));
        return topicConfigTable;
    }


    private String encode(Map<String, TopicConfig> topicConfigTable) {
        if (!topicConfigTable.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (TopicConfig config : topicConfigTable.values()) {
                sb.append(config.getTopicName() + "=" + config.encode() + IOUtils.LINE_SEPARATOR);
            }

            return sb.toString();
        }

        return null;
    }
}
