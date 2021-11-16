package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.HeartbeatService;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class HeartbeatServiceTest {

    @Test
    public void test() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        HeartbeatService service = new HeartbeatService(clientConfig.getHeartbeatBrokerInterval());
        service.start();
        int n = 100;
        CountDownLatch countDownLatch = new CountDownLatch(1);

        for (int i = 0; i < n; i++) {
            int finalI = i;
            service.addHeartbeatTask(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (finalI == n - 1) {
                    countDownLatch.countDown();
                }
            });
            Thread.sleep(1);
        }
        countDownLatch.await(1, TimeUnit.SECONDS);
        service.shutdown();
        long count = countDownLatch.getCount();
        assertThat(count).isEqualTo(0);
    }
}
