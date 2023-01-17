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
package org.apache.rocketmq.controller.impl.controller.processor;

import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ControllerRequestProcessorTest {

    @Mock
    private ChannelHandlerContext ctx;


    private ControllerRequestProcessor controllerRequestProcessor;

    @Mock
    private ControllerManager controllerManager;

    @Before
    public void startup() {
        controllerRequestProcessor = new ControllerRequestProcessor(controllerManager);
    }

   @Test
   public void testProcessRequest() throws Exception {
       int code = 1001;
       CommandCustomHeader header = new SampleCommandCustomHeader();
       RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
       Set<String> syncStateSet = new HashSet<>();
       int syncStateSetEpoch = 0;
       SyncStateSet ss = new SyncStateSet(syncStateSet,syncStateSetEpoch);
       cmd.setBody(ss.toJson().getBytes(StandardCharsets.UTF_8));
       RemotingCommand remotingCommand = controllerRequestProcessor.processRequest(ctx,cmd);
       System.out.println(remotingCommand);
   }

    static class SampleCommandCustomHeader implements CommandCustomHeader {
        @Override
        public void checkFields() throws RemotingCommandException {
        }
    }
}
