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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.consumer.pop;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class ChangeInvisibleTimeMidMsgOrderlyIT extends BasePopOrderly {
    /**
     * send three messages (msg1, msg2, msg3, msg4) and the max message num of pop request is three
     * <p>
     * ack msg1 and msg3, changeInvisibleTime msg2
     * <p>
     * expect the sequence of message received is: msg1, msg2, msg3, msg2, msg3, msg4
     */
    @Test
    public void test() {
        producer.send(4);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            changeInvisibleTimeMidMessage().get();
            return msgRecvSequence.size() == 6;
        });

        assertMsgRecv(0, 1);
        assertMsgRecv(1, 2);
        assertMsgRecv(2, 2);
        assertMsgRecv(5, 1);

        assertEquals(msgRecvSequence.get(1), msgRecvSequence.get(3));
        assertEquals(msgRecvSequence.get(2), msgRecvSequence.get(4));
    }

    private CompletableFuture<Void> changeInvisibleTimeMidMessage() {
        CompletableFuture<PopResult> future = client.popMessageAsync(
            brokerAddr, messageQueue, 5000, 3, group, POP_TIMEOUT, true,
            ConsumeInitMode.MIN, true, ExpressionType.TAG, "*");
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        future.whenComplete((popResult, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
                return;
            }
            if (popResult.getMsgFoundList() == null || popResult.getMsgFoundList().isEmpty()) {
                resultFuture.complete(null);
                return;
            }
            try {
                for (MessageExt messageExt : popResult.getMsgFoundList()) {
                    onRecvNewMessage(messageExt);
                    if (msgRecv.size() != 2) {
                        try {
                            client.ackMessageAsync(brokerAddr, topic, group, messageExt.getProperty(MessageConst.PROPERTY_POP_CK)).get();
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                            return;
                        }
                    } else {
                        try {
                            TimeUnit.MILLISECONDS.sleep(1);
                            client.changeInvisibleTimeAsync(
                                brokerAddr, BROKER1_NAME, topic, group,
                                messageExt.getProperty(MessageConst.PROPERTY_POP_CK), 3000).get();
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                            return;
                        }
                    }
                }
                resultFuture.complete(null);
            } catch (Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        });
        return resultFuture;
    }
}
