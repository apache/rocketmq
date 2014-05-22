package com.alibaba.rocketmq.research;

import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Test {

    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2013);
        cal.set(Calendar.MONTH, 10);
        cal.set(Calendar.DAY_OF_MONTH, 11);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }


    private static String diskUtil() {
        String storePathPhysic = System.getenv("HOME");
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis = storePathPhysic;
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = storePathPhysic;
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }


    public static void testlog() throws JoranException {
        // 初始化Logback
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure("/Users/vive/Desktop/share/work/gitlab/rocketmq/conf/logback_broker.xml");
        final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

        while (true) {
            log.info("hello, current time: {}", System.currentTimeMillis());
        }
    }


    public static void main(String[] args) throws JoranException {
        testlog();
    }
}
