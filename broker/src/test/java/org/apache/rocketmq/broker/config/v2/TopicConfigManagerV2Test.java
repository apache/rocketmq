package org.apache.rocketmq.broker.config.v2;

import java.io.File;
import java.io.IOException;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(value = MockitoJUnitRunner.class)
public class TopicConfigManagerV2Test {

    private ConfigStorage configStorage;

    private TopicConfigManagerV2 topicConfigManagerV2;

    @Mock
    private BrokerController controller;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @After
    public void cleanUp() {
        if (null != configStorage) {
            configStorage.shutdown();
        }
    }

    @Before
    public void setUp() throws IOException {
        BrokerConfig brokerConfig = new BrokerConfig();
        Mockito.doReturn(brokerConfig).when(controller).getBrokerConfig();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        Mockito.doReturn(messageStoreConfig).when(controller).getMessageStoreConfig();

        File configStoreDir = tf.newFolder();
        configStorage = new ConfigStorage(configStoreDir.getAbsolutePath());
        configStorage.start();
        topicConfigManagerV2 = new TopicConfigManagerV2(controller, configStorage);
    }

    @Test
    public void testUpdateTopicConfig() {
        TopicConfig topicConfig = new TopicConfig();
        String topicName = "T1";
        topicConfig.setTopicName(topicName);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(4);
        topicConfig.setOrder(true);
        topicConfig.setTopicSysFlag(4);
        topicConfigManagerV2.updateTopicConfig(topicConfig);

        Assert.assertTrue(configStorage.shutdown());

        topicConfigManagerV2.getTopicConfigTable().clear();

        Assert.assertTrue(configStorage.start());
        Assert.assertTrue(topicConfigManagerV2.load());

        TopicConfig loaded = topicConfigManagerV2.selectTopicConfig(topicName);
        Assert.assertNotNull(loaded);
        Assert.assertEquals(topicName, loaded.getTopicName());
        Assert.assertEquals(6, loaded.getPerm());
        Assert.assertEquals(8, loaded.getReadQueueNums());
        Assert.assertEquals(4, loaded.getWriteQueueNums());
        Assert.assertTrue(loaded.isOrder());
        Assert.assertEquals(4, loaded.getTopicSysFlag());

        Assert.assertTrue(topicConfigManagerV2.containsTopic(topicName));
    }

    @Test
    public void testRemoveTopicConfig() {
        TopicConfig topicConfig = new TopicConfig();
        String topicName = "T1";
        topicConfig.setTopicName(topicName);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(4);
        topicConfig.setOrder(true);
        topicConfig.setTopicSysFlag(4);
        topicConfigManagerV2.updateTopicConfig(topicConfig);
        topicConfigManagerV2.removeTopicConfig(topicName);
        Assert.assertFalse(topicConfigManagerV2.containsTopic(topicName));
        Assert.assertTrue(configStorage.shutdown());

        Assert.assertTrue(configStorage.start());
        Assert.assertTrue(topicConfigManagerV2.load());
        Assert.assertFalse(topicConfigManagerV2.containsTopic(topicName));
    }
}
