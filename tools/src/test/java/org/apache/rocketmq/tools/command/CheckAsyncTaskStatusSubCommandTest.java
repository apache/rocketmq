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

package org.apache.rocketmq.tools.command;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.AsyncTask;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckAsyncTaskStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.apache.rocketmq.tools.command.stats.CheckAsyncTaskStatusSubCommand;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;


public class CheckAsyncTaskStatusSubCommandTest extends ServerResponseMocker {

    @Mock
    private DefaultMQAdminExt defaultMQAdminExt;

    @Mock
    private CommandLine commandLine;

    @Mock
    private RPCHook rpcHook;

    private CheckAsyncTaskStatusSubCommand checkAsyncTaskStatusSubCommand;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        checkAsyncTaskStatusSubCommand = new CheckAsyncTaskStatusSubCommand();

        Field field = CheckAsyncTaskStatusSubCommand.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(checkAsyncTaskStatusSubCommand, defaultMQAdminExt);
    }

    @Override
    protected byte[] getBody() {
        return new byte[0];
    }

    @Test
    public void testExecute_Success() throws Exception {
        String clusterName = "testCluster";
        String taskName = "testTask";
        String brokerName = "brokerA";
        String brokerAddr = "127.0.0.1:" + listenPort();
        Integer taskStatus = 1;

        when(commandLine.hasOption('c')).thenReturn(true);
        when(commandLine.getOptionValue('c')).thenReturn(clusterName);
        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);
        when(commandLine.hasOption('s')).thenReturn(true);
        when(commandLine.getOptionValue('s')).thenReturn(String.valueOf(taskStatus));

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put(clusterName, new HashSet<>(Collections.singletonList(brokerName)));
        clusterInfo.setClusterAddrTable(clusterAddrTable);

        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, brokerAddr);
        when(brokerData.selectBrokerAddr()).thenReturn(brokerAddr);
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put(brokerName, brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        CompletableFuture<String> future = new CompletableFuture<>();
        AsyncTask asyncTask = new AsyncTask("testTask", "122421", future);
        asyncTask.setTaskId("taskId");
        asyncTask.setStatus(1);
        asyncTask.setResult("Task completed successfully");
        asyncTask.setCreateTime(new Date());

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        when(defaultMQAdminExt.checkAsyncTaskStatus(eq(brokerAddr), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenReturn(Collections.singletonList(asyncTask));
        verify(defaultMQAdminExt).shutdown();
    }

    @Test
    public void testExecute_EmptyTaskName() throws Exception {
        when(commandLine.hasOption('t')).thenReturn(false);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        System.setOut(originalOut);

        String output = outputStream.toString().trim();
        assertEquals("Task name cannot be empty. Please specify a task name with -t.", output);

        verify(defaultMQAdminExt, never()).start();
        verify(defaultMQAdminExt, never()).examineBrokerClusterInfo();
        verify(defaultMQAdminExt, never()).checkAsyncTaskStatus(anyString(), any(CheckAsyncTaskStatusRequestHeader.class));
        verify(defaultMQAdminExt, never()).shutdown();
    }

    @Test
    public void testExecute_WithBrokerAddr() throws Exception {
        String brokerAddr = "127.0.0.1:" + listenPort();
        String taskName = "testTask";

        when(commandLine.hasOption('b')).thenReturn(true);
        when(commandLine.getOptionValue('b')).thenReturn(brokerAddr);
        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, brokerAddr);
        when(brokerData.selectBrokerAddr()).thenReturn(brokerAddr);
        brokerAddrTable.put("brokerA", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        AsyncTask asyncTask = new AsyncTask(taskName, "taskId", new CompletableFuture<>());
        when(defaultMQAdminExt.checkAsyncTaskStatus(eq(brokerAddr), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenReturn(Collections.singletonList(asyncTask));

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt).checkAsyncTaskStatus(eq(brokerAddr), any(CheckAsyncTaskStatusRequestHeader.class));
        verify(defaultMQAdminExt).shutdown();
    }

    @Test
    public void testExecute_WithMaxLimit() throws Exception {
        String taskName = "testTask";
        int maxLimit = 5;

        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);
        when(commandLine.hasOption('m')).thenReturn(true);
        when(commandLine.getOptionValue('m')).thenReturn(String.valueOf(maxLimit));

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, "127.0.0.1:10911");
        when(brokerData.selectBrokerAddr()).thenReturn("127.0.0.1:10911");
        brokerAddrTable.put("brokerA", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        List<AsyncTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(new AsyncTask(taskName, "taskId" + i, new CompletableFuture<>()));
        }
        when(defaultMQAdminExt.checkAsyncTaskStatus(anyString(), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenReturn(tasks);

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt).checkAsyncTaskStatus(anyString(), argThat(header -> header.getMaxLimit() == maxLimit));
        verify(defaultMQAdminExt).shutdown();
    }

    @Test
    public void testExecute_WithTaskStatus() throws Exception {
        String taskName = "testTask";
        int taskStatus = 1;

        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);
        when(commandLine.hasOption('s')).thenReturn(true);
        when(commandLine.getOptionValue('s')).thenReturn(String.valueOf(taskStatus));

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, "127.0.0.1:10911");
        when(brokerData.selectBrokerAddr()).thenReturn("127.0.0.1:10911");
        brokerAddrTable.put("brokerA", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        AsyncTask asyncTask = new AsyncTask(taskName, "taskId", new CompletableFuture<>());
        asyncTask.setStatus(taskStatus);
        when(defaultMQAdminExt.checkAsyncTaskStatus(anyString(), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenReturn(Collections.singletonList(asyncTask));

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt).checkAsyncTaskStatus(anyString(), argThat(header -> header.getTaskStatus() == taskStatus));
        verify(defaultMQAdminExt).shutdown();
    }

    @Test
    public void testExecute_BrokerAddrNotFound() throws Exception {
        String brokerAddr = "127.0.0.1:10911";
        String taskName = "testTask";

        when(commandLine.hasOption('b')).thenReturn(true);
        when(commandLine.getOptionValue('b')).thenReturn(brokerAddr);
        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, "127.0.0.1:10900");
        when(brokerData.selectBrokerAddr()).thenReturn("127.0.0.1:10900");
        brokerAddrTable.put("brokerA", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);

        System.setOut(originalOut);

        String output = outputStream.toString().trim();
        assertTrue(output.contains("Broker with address '" + brokerAddr + "' not found."));

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt, never()).checkAsyncTaskStatus(anyString(), any(CheckAsyncTaskStatusRequestHeader.class));
        verify(defaultMQAdminExt).shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void testExecute_ExceptionThrown() throws Exception {
        String clusterName = "testCluster";
        String taskName = "testTask";

        when(commandLine.hasOption('c')).thenReturn(true);
        when(commandLine.getOptionValue('c')).thenReturn(clusterName);
        when(commandLine.hasOption('t')).thenReturn(true);
        when(commandLine.getOptionValue('t')).thenReturn(taskName);

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put(clusterName, new HashSet<>(Collections.singletonList("brokerA")));
        clusterInfo.setClusterAddrTable(clusterAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        when(defaultMQAdminExt.checkAsyncTaskStatus(anyString(), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenThrow(new RuntimeException("Broker communication error"));

        try {
            checkAsyncTaskStatusSubCommand.execute(commandLine, new Options(), rpcHook);
        } catch (RuntimeException e) {
            assertEquals(
                "Failed to execute " + checkAsyncTaskStatusSubCommand.getClass().getSimpleName() + " command",
                e.getMessage()
            );
            throw e;
        }

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt).shutdown();
    }

    @Test
    public void testExecuteWithCommandLineArgs() throws Exception {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t", "testTask"};
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqadmin " + checkAsyncTaskStatusSubCommand.commandName(), subargs,
            checkAsyncTaskStatusSubCommand.buildCommandlineOptions(options), new DefaultParser()
        );

        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put("defaultCluster", new HashSet<>(Collections.singletonList("brokerA")));
        clusterInfo.setClusterAddrTable(clusterAddrTable);

        BrokerData brokerData = spy(new BrokerData());
        brokerData.setBrokerAddrs(new HashMap<>());
        brokerData.getBrokerAddrs().put(0L, "127.0.0.1:10911");
        when(brokerData.selectBrokerAddr()).thenReturn("127.0.0.1:10911");
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put("brokerA", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);

        CompletableFuture<String> future = new CompletableFuture<>();
        AsyncTask asyncTask = new AsyncTask("testTask", "122421", future);
        asyncTask.setTaskId("taskId");
        asyncTask.setStatus(1);
        asyncTask.setResult("Task completed successfully");
        asyncTask.setCreateTime(new Date());

        when(defaultMQAdminExt.checkAsyncTaskStatus(eq("127.0.0.1:10911"), any(CheckAsyncTaskStatusRequestHeader.class)))
            .thenReturn(Collections.singletonList(asyncTask));

        checkAsyncTaskStatusSubCommand.execute(commandLine, options, rpcHook);

        verify(defaultMQAdminExt).start();
        verify(defaultMQAdminExt).examineBrokerClusterInfo();
        verify(defaultMQAdminExt).checkAsyncTaskStatus(eq("127.0.0.1:10911"), any(CheckAsyncTaskStatusRequestHeader.class));
        verify(defaultMQAdminExt).shutdown();
    }
}
