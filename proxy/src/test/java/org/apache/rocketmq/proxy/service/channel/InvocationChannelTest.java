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

package org.apache.rocketmq.proxy.service.channel;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InvocationChannelTest {

    @Test
    public void testWriteAndFlushShouldNotRemoveReRegisteredContext() {
        InvocationChannel channel = new InvocationChannel("127.0.0.1:8080", "127.0.0.1:8081");
        AtomicBoolean nextContextHandled = new AtomicBoolean(false);

        channel.registerInvocationContext(1, new InvocationContextInterface() {
            @Override
            public void handle(RemotingCommand remotingCommand) {
                channel.registerInvocationContext(remotingCommand.getOpaque(), new InvocationContextInterface() {
                    @Override
                    public void handle(RemotingCommand nextRemotingCommand) {
                        nextContextHandled.set(true);
                    }

                    @Override
                    public boolean expired(long expiredTimeSec) {
                        return false;
                    }
                });
            }

            @Override
            public boolean expired(long expiredTimeSec) {
                return false;
            }
        });

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "OK");
        response.setOpaque(1);

        channel.writeAndFlush(response);
        assertTrue(channel.isWritable());

        channel.writeAndFlush(response);
        assertTrue(nextContextHandled.get());
        assertFalse(channel.isWritable());
    }
}
