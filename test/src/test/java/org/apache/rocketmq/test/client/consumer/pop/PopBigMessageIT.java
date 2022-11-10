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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressorFactory;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class PopBigMessageIT extends BasePopNormally {

    private static final int BODY_LEN = 3 * 1024 * 1024;

    static {
        System.setProperty(ClientConfig.DECODE_DECOMPRESS_BODY, "false");
    }

    private Message createBigMessage() {
        byte[] bytes = new byte[BODY_LEN];
        return new Message(topic, bytes);
    }

    @Test
    public void testSendAndRecvBigMsgWhenDisablePopBufferMerge() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(false);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(false);

        this.testSendAndRecvBigMsg();
    }

    @Test
    public void testSendAndRecvBigMsgWhenEnablePopBufferMerge() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(true);

        this.testSendAndRecvBigMsg();
    }

    /**
     * set DECODE_DECOMPRESS_BODY to false, then pop message from broker and not ack
     * <p>
     * expect when re-consume this message, the message is not decompressed
     */
    private void testSendAndRecvBigMsg() {
        Message message = createBigMessage();
        producer.send(message);

        AtomicReference<MessageExt> firstMessageExtRef = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            PopResult popResult = popMessageAsync(Duration.ofSeconds(3).toMillis(), 1, 5000).get();
            assertEquals(PopStatus.FOUND, popResult.getPopStatus());

            firstMessageExtRef.set(popResult.getMsgFoundList().get(0));
            MessageExt messageExt = firstMessageExtRef.get();
            assertMessageRecv(messageExt);
        });

        // no ack, msg will put into pop retry topic
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            PopResult retryPopResult = popMessageAsync(Duration.ofSeconds(3).toMillis(), 1, 5000).get();
            assertEquals(PopStatus.FOUND, retryPopResult.getPopStatus());

            MessageExt retryMessageExt = retryPopResult.getMsgFoundList().get(0);
            assertMessageRecv(retryMessageExt);
            assertEquals(firstMessageExtRef.get().getBody().length, retryMessageExt.getBody().length);
        });
    }

    private void assertMessageRecv(MessageExt messageExt) throws IOException {
        assertEquals(MessageSysFlag.COMPRESSED_FLAG, messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG);
        Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(messageExt.getSysFlag()));
        assertEquals(BODY_LEN, compressor.decompress(messageExt.getBody()).length);
    }
}
