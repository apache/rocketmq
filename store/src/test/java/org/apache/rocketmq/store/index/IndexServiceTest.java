package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


public class IndexServiceTest {

    @Test
    public void testQueryOffsetThrow() throws Exception {
        assertDoesNotThrow(() -> {
            DefaultMessageStore store = new DefaultMessageStore(
                    new MessageStoreConfig(),
                    new BrokerStatsManager(new BrokerConfig()),
                    null,
                    new BrokerConfig(),
                    new ConcurrentHashMap<>()
            );

            IndexService indexService = new IndexService(store);
            indexService.queryOffset("test", "", Integer.MAX_VALUE, 10, 100);
        });
    }

}
