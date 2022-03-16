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

package org.apache.rocketmq.jmh;

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RemotingCommandJMHTest {

    private static String genString(int length) {
        byte[] array = new byte[length];
        new Random().nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }

    @State(Scope.Benchmark)
    static class RequestHolder {
        public static final SendMessageRequestHeader SEND_MESSAGE_REQUEST_HEADER = new SendMessageRequestHeader();

        static {
            SEND_MESSAGE_REQUEST_HEADER.setProducerGroup(genString(5));
            SEND_MESSAGE_REQUEST_HEADER.setTopic(genString(15));
            SEND_MESSAGE_REQUEST_HEADER.setDefaultTopic(genString(5));
            SEND_MESSAGE_REQUEST_HEADER.setDefaultTopicQueueNums(1);
            SEND_MESSAGE_REQUEST_HEADER.setQueueId(1);
            SEND_MESSAGE_REQUEST_HEADER.setSysFlag(1);
            SEND_MESSAGE_REQUEST_HEADER.setBornTimestamp(System.currentTimeMillis());
            SEND_MESSAGE_REQUEST_HEADER.setFlag(10);
            SEND_MESSAGE_REQUEST_HEADER.setProperties(genString(30));
            SEND_MESSAGE_REQUEST_HEADER.setReconsumeTimes(1010);
            SEND_MESSAGE_REQUEST_HEADER.setUnitMode(false);
            SEND_MESSAGE_REQUEST_HEADER.setMaxReconsumeTimes(111);
            SEND_MESSAGE_REQUEST_HEADER.setBatch(true);
        }
    }

    @State(Scope.Benchmark)
    static class RemotingCommandHolder {
        public static final RemotingCommand REMOTING_COMMAND = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, RequestHolder.SEND_MESSAGE_REQUEST_HEADER);

        static {
            REMOTING_COMMAND.makeCustomHeaderToNet();
        }

    }

    @Benchmark
    public CommandCustomHeader testDecodeCustomHeader() throws RemotingCommandException {
        return RemotingCommandHolder.REMOTING_COMMAND.decodeCommandCustomHeader(SendMessageRequestHeader.class);
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .detectJvmArgs()
                .include(RemotingCommandJMHTest.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

}
