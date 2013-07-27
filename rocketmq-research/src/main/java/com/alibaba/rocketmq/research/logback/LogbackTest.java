/**
 * $Id: LogbackTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.logback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class LogbackTest {
    public final static Logger log = LoggerFactory.getLogger(LogbackTest.class);


    /**
     * @param args
     */
    public static void main(String[] args) {
        int a = 100;
        long b = 300L;
        double c = 99.8;

        log.info("a {} b {} c {}", a, b, c);
    }
}
