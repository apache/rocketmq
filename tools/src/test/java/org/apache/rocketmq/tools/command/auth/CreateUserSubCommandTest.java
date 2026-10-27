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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateUserSubCommandTest {
    private static final String BROKER = "127.0.0.1:10911";
    private static final String USERNAME = "bob";
    private static final String PASSWORD = "  s3cret  ";

    private DefaultMQAdminExt adminExt;
    private CommandLine commandLine;
    private CreateUserSubCommand command;

    @Before
    public void setUp() {
        adminExt = mock(DefaultMQAdminExt.class);
        commandLine = mock(CommandLine.class);
        command = new CreateUserSubCommand() {
            @Override
            DefaultMQAdminExt createAdminExt(RPCHook rpcHook) {
                return adminExt;
            }
        };
        when(commandLine.hasOption('b')).thenReturn(true);
        when(commandLine.getOptionValue('b')).thenReturn(BROKER);
        when(commandLine.getOptionValue('u')).thenReturn(USERNAME);
        when(commandLine.getOptionValue('p')).thenReturn(PASSWORD);
    }

    @Test
    public void testCreateKeepsThePasswordVerbatim() throws Exception {
        command.execute(commandLine, new Options(), null);

        ArgumentCaptor<String> password = ArgumentCaptor.forClass(String.class);
        verify(adminExt).createUser(eq(BROKER), eq(USERNAME), password.capture(), any());
        Assert.assertEquals(PASSWORD, password.getValue());
    }
}
