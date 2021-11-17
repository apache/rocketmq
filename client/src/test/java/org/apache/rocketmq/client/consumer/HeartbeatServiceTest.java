/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
