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

package org.apache.rocketmq.acl.common;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestType;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.junit.Test;

import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SECURITY_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

public class AclClientRPCHookTest {
    protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache =
        new ConcurrentHashMap<>();
    private AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(null);

    @Test
    public void testParseRequestContent() {
        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup("group");
        requestHeader.setTopic("topic");
        requestHeader.setQueueId(1);
        requestHeader.setQueueOffset(2L);
        requestHeader.setMaxMsgNums(32);
        requestHeader.setSysFlag(0);
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(15000L);
        requestHeader.setSubVersion(0L);
        RemotingCommand testPullRemotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        SortedMap<String, String> oldContent = oldVersionParseRequestContent(testPullRemotingCommand, "ak", null);
        byte[] oldBytes = AclUtils.combineRequestContent(testPullRemotingCommand, oldContent);
        testPullRemotingCommand.addExtField(ACCESS_KEY, "ak");
        SortedMap<String, String> content = aclClientRPCHook.parseRequestContent(testPullRemotingCommand);
        byte[] newBytes = AclUtils.combineRequestContent(testPullRemotingCommand, content);
        assertThat(newBytes).isEqualTo(oldBytes);
    }

    @Test
    public void testParseRequestContentWithStreamRequestType() {
        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup("group");
        requestHeader.setTopic("topic");
        requestHeader.setQueueId(1);
        requestHeader.setQueueOffset(2L);
        requestHeader.setMaxMsgNums(32);
        requestHeader.setSysFlag(0);
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(15000L);
        requestHeader.setSubVersion(0L);
        RemotingCommand testPullRemotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        testPullRemotingCommand.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()));
        testPullRemotingCommand.addExtField(ACCESS_KEY, "ak");
        SortedMap<String, String> content = aclClientRPCHook.parseRequestContent(testPullRemotingCommand);
        assertThat(content.get(MixAll.REQ_T)).isEqualTo(String.valueOf(RequestType.STREAM.getCode()));
    }

    private SortedMap<String, String> oldVersionParseRequestContent(RemotingCommand request, String ak, String securityToken) {
        CommandCustomHeader header = request.readCustomHeader();
        // Sort property
        SortedMap<String, String> map = new TreeMap<>();
        map.put(ACCESS_KEY, ak);
        if (securityToken != null) {
            map.put(SECURITY_TOKEN, securityToken);
        }
        try {
            // Add header properties
            if (null != header) {
                Field[] fields = fieldCache.get(header.getClass());
                if (null == fields) {
                    fields = header.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                    }
                    Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
                    if (null != tmp) {
                        fields = tmp;
                    }
                }

                for (Field field : fields) {
                    Object value = field.get(header);
                    if (null != value && !field.isSynthetic()) {
                        map.put(field.getName(), value.toString());
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }
}
