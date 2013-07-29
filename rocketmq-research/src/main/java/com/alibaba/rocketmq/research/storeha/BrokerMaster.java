/**
 * $Id: BrokerMaster.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.storeha;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.research.store.MessageStoreTestObject;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * HA测试
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class BrokerMaster {

    public static void main(String[] args) {
        try {
            final String brokerRole = args.length >= 1 ? args[0] : "SYNC_MASTER";
            final int ThreadSize = args.length >= 2 ? Integer.parseInt(args[1]) : 128;

            MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            messageStoreConfig.setBrokerRole(brokerRole);

            final MessageStoreTestObject storeTestObject = new MessageStoreTestObject(messageStoreConfig);

            if (!storeTestObject.load()) {
                System.out.println("load store failed");
                System.exit(-1);
            }

            storeTestObject.start();

            System.out.println("waiting 5s for slave connect....");
            Thread.sleep(1000 * 5);
            System.out.println("wait over");

            // Thread pool
            final ThreadPoolExecutor executorSend =
                    (ThreadPoolExecutor) Executors.newFixedThreadPool(ThreadSize);

            final AtomicLong maxResponseTime = new AtomicLong(0);
            final AtomicLong sendTotalCnt = new AtomicLong(0);

            for (int i = 0; i < ThreadSize; i++) {
                executorSend.execute(new Runnable() {

                    @Override
                    public void run() {
                        for (long k = 1;; k++) {
                            try {
                                long beginTime = System.currentTimeMillis();
                                boolean result = storeTestObject.sendMessage();
                                long rt = System.currentTimeMillis() - beginTime;
                                if (rt > maxResponseTime.get()) {
                                    maxResponseTime.set(rt);
                                }

                                if (!result) {
                                    System.err.println(k + "\tSend message failed, error message:");
                                    Thread.sleep(1000);
                                }
                                else {
                                    sendTotalCnt.incrementAndGet();
                                }
                            }
                            catch (Exception e) {
                                System.out.println("sendMessage exception -------------");
                                e.printStackTrace();
                                try {
                                    Thread.sleep(1000);
                                }
                                catch (InterruptedException e1) {
                                }
                            }
                        }
                    }
                });
            }

            new Thread(new Runnable() {
                @Override
                public void run() {
                    long lastTime = System.currentTimeMillis();
                    long lastTotal = 0;
                    while (true) {
                        try {
                            Thread.sleep(1000 * 3);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        long now = System.currentTimeMillis();
                        long currentTotal = sendTotalCnt.get();

                        Double tps = (currentTotal - lastTotal) / ((now - lastTime) * 1.0);
                        tps *= 1000;

                        lastTime = now;
                        lastTotal = currentTotal;
                        double avtRT = 1000 / ((tps / ThreadSize) * 1.0);
                        System.out.println("send tps = " + tps.longValue() + " maxResponseTime(ms) = "
                                + maxResponseTime + " avgRT(ms) = " + avtRT);
                    }
                }
            }).start();

            System.out.println("start OK, " + messageStoreConfig.getBrokerRole());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
