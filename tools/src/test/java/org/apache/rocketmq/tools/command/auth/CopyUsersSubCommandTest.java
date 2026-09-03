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
package org.apache.rocketmq.tools.command.auth;

import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CopyUsersSubCommandTest {
    private static final String SOURCE_BROKER = "127.0.0.1:10911";
    private static final String TARGET_BROKER = "127.0.0.1:20911";

    private DefaultMQAdminExt adminExt;
    private CommandLine commandLine;
    private CopyUsersSubCommand command;

    @Before
    public void setUp() {
        adminExt = mock(DefaultMQAdminExt.class);
        commandLine = mock(CommandLine.class);
        command = new CopyUsersSubCommand() {
            @Override
            DefaultMQAdminExt createAdminExt(RPCHook rpcHook) {
                return adminExt;
            }
        };
        when(commandLine.hasOption("f")).thenReturn(true);
        when(commandLine.hasOption("t")).thenReturn(true);
        when(commandLine.getOptionValue("f")).thenReturn(SOURCE_BROKER);
        when(commandLine.getOptionValue("t")).thenReturn(TARGET_BROKER);
    }

    @Test
    public void testCopyAllFetchesCompleteUserBeforeCreate() throws Exception {
        UserInfo summary = UserInfo.of("alice", null, "Normal", "Enable");
        UserInfo complete = UserInfo.of("alice", "dummy-password", "Normal", "Enable");
        when(adminExt.listUser(SOURCE_BROKER, null)).thenReturn(Collections.singletonList(summary));
        when(adminExt.getUser(SOURCE_BROKER, "alice")).thenReturn(complete);
        when(adminExt.getUser(TARGET_BROKER, "alice")).thenReturn(null);

        command.execute(commandLine, new Options(), null);

        verify(adminExt).getUser(SOURCE_BROKER, "alice");
        verify(adminExt).createUser(TARGET_BROKER, complete);
    }

    @Test
    public void testCopyAllFetchesCompleteUserBeforeUpdate() throws Exception {
        UserInfo summary = UserInfo.of("alice", null, "Normal", "Enable");
        UserInfo complete = UserInfo.of("alice", "dummy-password", "Normal", "Enable");
        UserInfo target = UserInfo.of("alice", null, "Normal", "Enable");
        when(adminExt.listUser(SOURCE_BROKER, null)).thenReturn(Collections.singletonList(summary));
        when(adminExt.getUser(SOURCE_BROKER, "alice")).thenReturn(complete);
        when(adminExt.getUser(TARGET_BROKER, "alice")).thenReturn(target);

        command.execute(commandLine, new Options(), null);

        verify(adminExt).getUser(SOURCE_BROKER, "alice");
        verify(adminExt).updateUser(TARGET_BROKER, complete);
    }

    @Test
    public void testCopySelectedUserStillUsesCompleteUser() throws Exception {
        UserInfo complete = UserInfo.of("alice", "dummy-password", "Normal", "Enable");
        when(commandLine.getOptionValue('u')).thenReturn("alice");
        when(adminExt.getUser(SOURCE_BROKER, "alice")).thenReturn(complete);
        when(adminExt.getUser(TARGET_BROKER, "alice")).thenReturn(null);

        command.execute(commandLine, new Options(), null);

        verify(adminExt, never()).listUser(SOURCE_BROKER, null);
        verify(adminExt).createUser(TARGET_BROKER, complete);
    }

    @Test
    public void testCopyAllFailsWhenPasswordIsUnavailable() throws Exception {
        UserInfo summary = UserInfo.of("alice", null, "Normal", "Enable");
        when(adminExt.listUser(SOURCE_BROKER, null)).thenReturn(Collections.singletonList(summary));
        when(adminExt.getUser(SOURCE_BROKER, "alice")).thenReturn(summary);

        SubCommandException exception = Assert.assertThrows(SubCommandException.class,
            () -> command.execute(commandLine, new Options(), null));

        Assert.assertTrue(exception.getCause().getMessage().contains("Password is unavailable for user alice"));
        verify(adminExt, never()).getUser(TARGET_BROKER, "alice");
        verify(adminExt, never()).createUser(TARGET_BROKER, summary);
        verify(adminExt, never()).updateUser(TARGET_BROKER, summary);
    }
}
