package org.apache.rocketmq.store;

import org.junit.Test;

import static org.junit.Assert.*;

public class StoreStatsServiceTest {
    @Test
    public void StoreStatsServiceStringFromat() throws Exception {

        StoreStatsService storeStatsService = new StoreStatsService();
        new Thread(storeStatsService).start();
        assertNotNull(storeStatsService.toString());
    }

}