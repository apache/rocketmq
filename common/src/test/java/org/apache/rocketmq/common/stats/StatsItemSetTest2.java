package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class StatsItemSetTest2 {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);

    private StatsItemSet set;

    @Before
    public void init() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        set = new StatsItemSet("statsName" , scheduledExecutorService, log);
    }

    @Test
    public void testInit(){
        set.init();
    }

    @Test
    public void testAddValue() {
        set.addValue("statsKey", 10, 1);
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        Assert.assertEquals(10, itemTmp.getValue().intValue());
        Assert.assertEquals(1, itemTmp.getTimes().intValue());
    }

    @Test
    public void testGetAndCreateStatsItem() {
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        Assert.assertEquals("statsName", itemTmp.getStatsName());
        Assert.assertEquals("statsKey", itemTmp.getStatsKey());
    }

    @Test
    public void testGetStatsDataInMinute() {
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        StatsSnapshot snapshot1 = itemTmp.getStatsDataInMinute();
        StatsSnapshot snapshot2 = set.getStatsDataInMinute("statsKey");

        Assert.assertNotNull(snapshot1);
        Assert.assertNotNull(snapshot2);
        Assert.assertNotEquals(snapshot1, snapshot2);
    }

    @Test
    public void testGetStatsDataInHour() {
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        StatsSnapshot snapshot1 = itemTmp.getStatsDataInHour();
        StatsSnapshot snapshot2 = set.getStatsDataInHour("statsKey");

        Assert.assertNotNull(snapshot1);
        Assert.assertNotNull(snapshot2);
        Assert.assertNotEquals(snapshot1, snapshot2);
    }

    @Test
    public void testGetStatsDataInDay() {
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        StatsSnapshot snapshot1 = itemTmp.getStatsDataInDay();
        StatsSnapshot snapshot2 = set.getStatsDataInDay("statsKey");

        Assert.assertNotNull(snapshot1);
        Assert.assertNotNull(snapshot2);
        Assert.assertNotEquals(snapshot1, snapshot2);
    }

    @Test
    public void testGetStatsItem() {
        StatsItem itemTmp = set.getAndCreateStatsItem("statsKey");
        StatsItem item = set.getStatsItem("statsKey");
        Assert.assertEquals(itemTmp, item);
    }
}
