package com.alibaba.rocketmq.namesrv.daemon;

import static com.alibaba.rocketmq.namesrv.DataUtils.createExpect;
import static com.alibaba.rocketmq.namesrv.DataUtils.createSpecific1;
import static com.alibaba.rocketmq.namesrv.DataUtils.createSpecific2;
import static com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode.SUCCESS_VALUE;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.namesrv.DataUtils;
import com.alibaba.rocketmq.namesrv.common.Result;
import com.alibaba.rocketmq.namesrv.sync.FutureGroup;
import com.alibaba.rocketmq.namesrv.topic.DefaultTopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @auther lansheng.zj@taobao.com
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("com.taobao.metaq.common.MetaMix")
@PrepareForTest({ RemotingHelper.class })
public class NamesrvSyncTest {

    @Test
    public void testGroupCommit() throws Exception {
        TopicRuntimeData expected = createSpecific1();
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(SUCCESS_VALUE);
        response.setBody(expected.encode());

        NamesrvConfig namesrvConf = new NamesrvConfig();
        namesrvConf.setNamesrvAddr("meta://10.12.12.11:9876;meta://10.12.12.16:9876;");
        DefaultTopicRuntimeDataManager topicRuntimeDataManager = new DefaultTopicRuntimeDataManager(namesrvConf);
        NamesrvSync namesrvSync = new NamesrvSync(namesrvConf, topicRuntimeDataManager);

        PowerMock.mockStatic(RemotingHelper.class);
        expect(RemotingHelper.invokeSync((String) anyObject(), (RemotingCommand) anyObject(), anyLong()))
            .andReturn(response);
        PowerMock.replay(RemotingHelper.class);

        FutureGroup<Object> futureGroup = namesrvSync.groupCommit();
        Result result = futureGroup.await(namesrvConf.getGroupWaitTimeout());

        TopicRuntimeData topicRuntimeData =
                DataUtils.getField(topicRuntimeDataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(result.isSuccess());
        Assert.assertTrue(expected.equals(topicRuntimeData));
    }


    @Test
    public void testGroupCommitMerge() throws Exception {
        TopicRuntimeData expected = createExpect();
        TopicRuntimeData specific1 = createSpecific1();
        TopicRuntimeData specific2 = createSpecific2();

        RemotingCommand response1 = RemotingCommand.createResponseCommand(null);
        response1.setCode(SUCCESS_VALUE);
        response1.setBody(specific1.encode());
        RemotingCommand response2 = RemotingCommand.createResponseCommand(null);
        response2.setCode(SUCCESS_VALUE);
        response2.setBody(specific2.encode());

        NamesrvConfig namesrvConf = new NamesrvConfig();
        namesrvConf.setNamesrvAddr("addr1;addr2");
        namesrvConf.setSyncTimeout(100L);
        DefaultTopicRuntimeDataManager topicRuntimeDataManager = new DefaultTopicRuntimeDataManager(namesrvConf);
        NamesrvSync namesrvSync = new NamesrvSync(namesrvConf, topicRuntimeDataManager);

        PowerMock.mockStatic(RemotingHelper.class);
        expect(
            RemotingHelper.invokeSync(eq("addr1"), anyObject(RemotingCommand.class),
                eq(namesrvConf.getSyncTimeout()))).andReturn(response1).anyTimes();
        expect(
            RemotingHelper.invokeSync(eq("addr2"), anyObject(RemotingCommand.class),
                eq(namesrvConf.getSyncTimeout()))).andReturn(response2).anyTimes();
        PowerMock.replay(RemotingHelper.class);

        FutureGroup<Object> futureGroup = namesrvSync.groupCommit();
        Result result = futureGroup.await(namesrvConf.getGroupWaitTimeout());

        TopicRuntimeData topicRuntimeData =
                DataUtils.getField(topicRuntimeDataManager, TopicRuntimeData.class, "topicData");

        Assert.assertTrue(result.isSuccess());
        Assert.assertTrue(expected.equals(topicRuntimeData));
    }

}
