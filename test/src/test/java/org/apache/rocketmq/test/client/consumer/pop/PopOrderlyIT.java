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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class PopOrderlyIT extends BasePopOrderly {

    /**
     * send 10 messages, pop one message orderly at a time
     * <p>
     * expect receive this 10 messages in order
     */
    @Test
    public void testPopOrderly() {
        sendMessage(10);
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            popMessageOrderly().get();
            return msgRecv.size() == 10;
        });

        assertMessageRecvOrder();
    }

    private CompletableFuture<Void> popMessageOrderly() {
        CompletableFuture<PopResult> future = popMessageOrderlyAsync(TimeUnit.SECONDS.toMillis(3), 1, TimeUnit.SECONDS.toMillis(30));
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
                    // ack later
                    // expect when the lock is free, pop message request can receive messages immediately after ack
                    new Thread(() -> {
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                        ackMessageAsync(messageExt);
                    }).start();
                }
                resultFuture.complete(null);
            } catch (Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        });
        return resultFuture;
    }

    /**
     * send 10 messages, pop five messages orderly at a time
     * <p>
     * expect only receive the first five messages
     */
    @Test
    public void testPopOrderlyThenNoAck() {
        sendMessage(10);
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            popOrderlyThenNoAck().get();
            return msgRecvSequence.size() == 10;
        });
        assertEquals(5, msgRecv.size());
        for (Map.Entry<String, List<MsgRcv>> entry : msgRecv.entrySet()) {
            assertEquals(2, entry.getValue().size());
            for (int i = 0; i < entry.getValue().size(); i++) {
                assertEquals(i, entry.getValue().get(i).messageExt.getReconsumeTimes());
            }
        }
        assertMessageRecvOrder();
    }

    private CompletableFuture<Void> popOrderlyThenNoAck() {
        CompletableFuture<PopResult> future = popMessageOrderlyAsync(TimeUnit.SECONDS.toMillis(3), 5, TimeUnit.SECONDS.toMillis(30));
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
                }
                resultFuture.complete(null);
            } catch (Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        });
        return resultFuture;
    }

    /**
     * send one message, changeInvisibleTime to 5s later at the first time
     * <p>
     * expect receive two times
     */
    @Test
    public void testPopMessageOrderlyThenChangeInvisibleTime() {
        sendMessage(1);

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            popMessageOrderlyThenChangeInvisibleTime().get();
            return msgRecvSequence.size() == 2;
        });

        assertMsgRecv(1, 2);
        assertMessageRecvOrder();
    }

    private CompletableFuture<Void> popMessageOrderlyThenChangeInvisibleTime() {
        CompletableFuture<PopResult> future = popMessageOrderlyAsync(TimeUnit.SECONDS.toMillis(3), 1, TimeUnit.SECONDS.toMillis(30));
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
                    if (msgRecvSequence.size() == 1) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(1);
                            changeInvisibleTimeAsync(messageExt, 5000).get();
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                            return;
                        }
                    } else {
                        try {
                            ackMessageAsync(messageExt).get();
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

    /**
     * send three messages (msg1, msg2, msg3, msg4) and the max message num of pop request is three
     * <p>
     * ack msg1 and msg3, changeInvisibleTime msg2
     * <p>
     * expect the sequence of messages received is: msg1, msg2, msg3, msg2, msg3, msg4
     */
    @Test
    public void testPopMessageOrderlyThenChangeInvisibleTimeMidMessage() {
        producer.send(4);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            popMessageOrderlyThenChangeInvisibleTimeMidMessage().get();
            return msgRecvSequence.size() == 6;
        });

        assertMsgRecv(0, 1);
        assertMsgRecv(1, 2);
        assertMsgRecv(2, 2);
        assertMsgRecv(5, 1);

        assertEquals(msgRecvSequence.get(1), msgRecvSequence.get(3));
        assertEquals(msgRecvSequence.get(2), msgRecvSequence.get(4));
    }

    private CompletableFuture<Void> popMessageOrderlyThenChangeInvisibleTimeMidMessage() {
        CompletableFuture<PopResult> future = popMessageOrderlyAsync(5000, 3, TimeUnit.SECONDS.toMillis(30));
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
                            ackMessageAsync(messageExt).get();
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                            return;
                        }
                    } else {
                        try {
                            TimeUnit.MILLISECONDS.sleep(1);
                            changeInvisibleTimeAsync(messageExt, 3000).get();
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

    @Test
    public void testReentrant() {
        producer.send(1);

        popMessageForReentrant(null).join();
        assertMsgRecv(0, 1, Lists.newArrayList(0));

        String attemptId01 = "attemptId-01";
        popMessageForReentrant(attemptId01).join();
        assertMsgRecv(0, 2, Lists.newArrayList(0, 1));
        popMessageForReentrant(attemptId01).join();
        assertMsgRecv(0, 3, Lists.newArrayList(0, 1, 1));

        String attemptId02 = "attemptId-02";
        await().atLeast(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15)).until(() -> {
            popMessageForReentrant(attemptId02).join();
            return msgRecvSequence.size() == 4;
        });
        popMessageForReentrant(attemptId02).join();
        assertMsgRecv(0, 5, Lists.newArrayList(0, 1, 1, 2, 2));

        await().atLeast(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15)).until(() -> {
            popMessageForReentrant(null).join();
            return msgRecvSequence.size() == 6;
        });
        assertMsgRecv(0, 6, Lists.newArrayList(0, 1, 1, 2, 2, 3));
    }

    private CompletableFuture<Void> popMessageForReentrant(String attemptId) {
        return popMessageOrderlyAsync(TimeUnit.SECONDS.toMillis(10), 3, TimeUnit.SECONDS.toMillis(30), attemptId)
            .thenAccept(popResult -> {
                for (MessageExt messageExt : popResult.getMsgFoundList()) {
                    onRecvNewMessage(messageExt);
                }
            });
    }
}
