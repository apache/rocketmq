package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.client.common.ThreadLocalIndex;
import org.junit.Test;

public class ThreadLocalIndexTest {
    @Test
    public void test_getAndIncrement() throws InterruptedException {
        final ThreadLocalIndex index = new ThreadLocalIndex(10);


        for (int i = 0; i < 10; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        System.out.println(String.format("%s %s", Thread.currentThread().getName(), index.getAndIncrement()));
                    }
                }
            });

            th.setDaemon(true);
            th.start();
        }

        Thread.sleep(3000);
    }

}
