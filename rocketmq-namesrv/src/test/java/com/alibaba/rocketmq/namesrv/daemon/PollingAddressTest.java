package com.alibaba.rocketmq.namesrv.daemon;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.namesrv.DataUtils;
import com.alibaba.rocketmq.namesrv.processor.ChangeSpreadProcessor;
import com.alibaba.rocketmq.namesrv.sync.Task;
import com.alibaba.rocketmq.namesrv.sync.TaskGroup;
import com.alibaba.rocketmq.namesrv.sync.TaskType;
import com.alibaba.rocketmq.namesrv.topic.DefaultTopicRuntimeDataManager;


/**
 * @auther lansheng.zj@taobao.com
 */
public class PollingAddressTest {

    @Test
    public void testSetAddrAndFireChange() throws Exception {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        DefaultTopicRuntimeDataManager dataManager = new DefaultTopicRuntimeDataManager(namesrvConfig);
        NamesrvSync namesrvSync = new NamesrvSync(namesrvConfig, dataManager);
        namesrvSync.init();

        ChangeSpreadProcessor changeSpread = new ChangeSpreadProcessor(namesrvConfig);
        changeSpread.init();

        PollingAddress pollingAddress = new PollingAddress(namesrvConfig);
        pollingAddress.setAddrAndFireChange("12.123.12.31:9876;12.123.12.98:9876;");

        TaskGroup taskGroup = DataUtils.getField(namesrvSync, TaskGroup.class, "syncTaskGroup");
        Task task = taskGroup.getTask("12.123.12.31:9876");
        Assert.assertTrue(null != task);

        Map map = DataUtils.getField(changeSpread, Map.class, "taskGroupMap");
        taskGroup = (TaskGroup) map.get(TaskType.REG_BROKER);
        task = taskGroup.getTask("12.123.12.98:9876");
        Assert.assertTrue(null != task);
    }
}
