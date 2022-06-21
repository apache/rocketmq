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

package org.apache.rocketmq.test.smoke;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.AbstractRpcHook;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RPCHookContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.checkerframework.checker.units.qual.A;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class RPCHookIT extends BaseConf {
    private static Logger logger = Logger.getLogger(RPCHookIT.class);
    private DefaultMQProducer producer = null;
    private String topic = null;

    private DynamicRPCHook clientHook = new DynamicRPCHook();
    private DynamicRPCHook serverHook = new DynamicRPCHook();

    public static final int RPCHOOK_STOP_CODE = -1000;

    class DynamicRPCHook extends AbstractRpcHook {

        private RPCHookContext.Decision decision = RPCHookContext.Decision.CONTINUE;

        public RPCHookContext.Decision getDecision() {
            return decision;
        }

        public void setDecision(RPCHookContext.Decision decision) {
            this.decision = decision;
        }

        @Override public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
            if (request.getCode() == RequestCode.SEND_MESSAGE
                || request.getCode() == RequestCode.SEND_MESSAGE_V2) {
                if (getContext() != null) {
                    getContext().setDecision(decision);
                    getContext().setResponseFuture(CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(RPCHOOK_STOP_CODE,
                        "STOP BY the HOOK")));
                }
            }
        }
    }

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic, false, clientHook).getProducer();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setRetryTimesWhenSendFailed(0);
    }



    @After
    public void tearDown() {
        super.shutdown();
    }


    @Test
    public void testServerRPCHook() throws Exception {
        try {
            for (BrokerController brokerController : getControllers()) {
                brokerController.registerServerRPCHook(serverHook);
            }
            doTestRPCHook(serverHook);
        } finally {
            serverHook.setDecision(RPCHookContext.Decision.CONTINUE);
        }
    }

    @Test
    public void testClientRPCHook() throws Exception {
        doTestRPCHook(clientHook);
    }


    private void doTestRPCHook(DynamicRPCHook rpcHook) throws Exception {
        {
            rpcHook.setDecision(RPCHookContext.Decision.CONTINUE);
            for (Object msg: MessageFactory.getRandomMessageList(topic, 10)) {
                SendResult result = producer.send((Message) msg);
                Assert.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
            }
            for (Object msg: MessageFactory.getRandomMessageList(topic, 10)) {
                CompletableFuture<SendResult> future  = new CompletableFuture<>();
                producer.send((Message) msg, new SendCallback() {
                    @Override public void onSuccess(SendResult sendResult) {
                        future.complete(sendResult);
                    }

                    @Override public void onException(Throwable e) {
                        future.completeExceptionally(e);
                    }
                });
                Assert.assertEquals(SendStatus.SEND_OK, future.get().getSendStatus());
            }
        }

        {
            rpcHook.setDecision(RPCHookContext.Decision.STOP);
            for (Object msg: MessageFactory.getRandomMessageList(topic, 10)) {
                MQBrokerException exception = null;
                try {
                    producer.send((Message) msg);
                } catch (MQBrokerException e) {
                    exception = e;
                }
                Assert.assertNotNull(exception);
                Assert.assertEquals(RPCHOOK_STOP_CODE, exception.getResponseCode());
            }
            for (Object msg: MessageFactory.getRandomMessageList(topic, 10)) {
                CompletableFuture<SendResult> future  = new CompletableFuture<>();
                producer.send((Message) msg, new SendCallback() {
                    @Override public void onSuccess(SendResult sendResult) {
                        future.complete(sendResult);
                    }

                    @Override public void onException(Throwable e) {
                        future.completeExceptionally(e);
                    }
                });

                MQBrokerException exception = null;
                try {
                    future.get();
                } catch (ExecutionException e) {
                    exception = (MQBrokerException) e.getCause();
                }
                Assert.assertNotNull(exception);
                Assert.assertEquals(RPCHOOK_STOP_CODE, exception.getResponseCode());
            }
        }
    }
}
