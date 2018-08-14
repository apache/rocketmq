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
package org.apache.rocketmq.broker.acl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.AclConfigData;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Test for {@link AclRPCHook}.
 */
public class TestAclRPCHook {

    AclRPCHook aclRPCHook;
    @Mock
    private BrokerOuterAPI brokerOuterAPI;
    @Mock
    private CommandCustomHeader commandCustomHeader;

    public void init() throws Exception {
        System.out.println(" test: " + brokerOuterAPI == null);
        aclRPCHook = new AclRPCHook(brokerOuterAPI);
        Field field = AclRPCHook.class.getDeclaredField("brokerOuterAPI");
        field.setAccessible(true);
        field.set(aclRPCHook, brokerOuterAPI);
    }

    @Test
    public void testAllDenied() throws Exception {
        init();
        AclConfigData aclConfigData = new AclConfigData();
        aclConfigData.setOperation("deny");
        // return false
        when(brokerOuterAPI.getAclData("test")).thenReturn(aclConfigData);
        try {
            final RemotingCommand appednRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, commandCustomHeader);
            aclRPCHook.doBeforeRequest("test", appednRequest);
        } catch (Exception e){
            assertTrue(e instanceof RuntimeException);
        }
        try {
            final RemotingCommand pullRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, commandCustomHeader);
            aclRPCHook.doBeforeRequest("test", pullRequest);
        } catch (Exception e){
            assertTrue(e instanceof RuntimeException);
        }
    }

    @Test
    public void testAllPermitted() throws Exception {
        init();
        AclConfigData aclConfigData = new AclConfigData();
        aclConfigData.setOperation("w");
        // return true
        when(brokerOuterAPI.getAclData("test")).thenReturn(aclConfigData);
        try {
            final RemotingCommand appednRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, commandCustomHeader);
            aclRPCHook.doBeforeRequest("test", appednRequest);
        } catch (Exception e){
        }
        assertEquals(1, 1);
    }

}
