package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.TransactionMetrics;
import org.apache.rocketmq.broker.transaction.TransactionMetrics.Metric;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@RunWith(MockitoJUnitRunner.class)
public class TransactionMetricsTest {
    private TransactionMetrics transactionMetrics;
    private ConcurrentMap<String, Metric> transactionCounts = new ConcurrentHashMap<>();
    private String configPath;

    @Before
    public void setUp() throws Exception {
        configPath = "configPath";
        transactionMetrics = new TransactionMetrics(configPath);
    }

    /**
     * 测试addAndGet方法
     */
    @Test
    public void testAddAndGet() {
        String topic = "testAddAndGet";
        int value = 10;
        long result = transactionMetrics.addAndGet(topic, value);

        assert result == value;
    }

    /**
     * 测试获取主题对应的度量值
     */
    @Test
    public void testGetTopicPair() {
        String topic = "getTopicPair";
        Metric result = transactionMetrics.getTopicPair(topic);
        assert result != null;
    }

    /**
     * 测试获取事务计数
     */
    @Test
    public void testGetTransactionCount() {
        String topicExist = "topicExist";
        String topicNotExist = "topicNotExist";

        transactionMetrics.addAndGet(topicExist, 10);

        assert transactionMetrics.getTransactionCount(topicExist) == 10;
        assert transactionMetrics.getTransactionCount(topicNotExist) == 0;
    }


    /**
     * 测试清除指标
     */
    @Test
    public void testCleanMetrics() {
        String topic = "testCleanMetrics";
        int value = 10;
        assert transactionMetrics.addAndGet(topic, value) == value;
        transactionMetrics.cleanMetrics(Collections.singleton(topic));
        assert transactionMetrics.getTransactionCount(topic) == 0;
    }
}
