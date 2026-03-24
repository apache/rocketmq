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
package org.apache.rocketmq.remoting.netty;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Ignore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for RemotingCodeDistributionHandler traffic recording overhead.
 * <p>
 * Benchmarks call recordInbound() directly (package-private) to isolate
 * recording overhead from Netty pipeline propagation cost.
 * <p>
 * Use @Param to compare detailed-off (O(1)) vs detailed-on (O(n)) modes.
 * <p>
 * Run via IDE: execute the main method.
 */
@Ignore
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class RemotingCodeDistributionBenchmark {

    /**
     * Shared handler state - handler itself is thread-safe (ConcurrentHashMap + LongAdder).
     */
    @State(Scope.Benchmark)
    public static class HandlerState {
        RemotingCodeDistributionHandler handler;
        NettyServerConfig config;

        @Param({"false", "true"})
        boolean enableDetailedTrafficSize;

        @Setup
        public void setup() {
            config = new NettyServerConfig();
            config.setEnableDetailedTrafficSize(enableDetailedTrafficSize);
            handler = new RemotingCodeDistributionHandler(config);
        }
    }

    /**
     * Per-thread commands to avoid contention on RemotingCommand fields.
     */
    @State(Scope.Thread)
    public static class CommandState {
        RemotingCommand cmdNoBody;
        RemotingCommand cmdWithBody;
        RemotingCommand cmdWithBodyAndExtFields;

        @Setup
        public void setup() {
            cmdNoBody = RemotingCommand.createRequestCommand(10, null);

            cmdWithBody = RemotingCommand.createRequestCommand(11, null);
            cmdWithBody.setBody(new byte[4096]);

            cmdWithBodyAndExtFields = RemotingCommand.createRequestCommand(12, null);
            cmdWithBodyAndExtFields.setBody(new byte[4096]);
            cmdWithBodyAndExtFields.setRemark("benchmark remark");
            HashMap<String, String> extFields = new HashMap<>();
            extFields.put("topic", "BenchmarkTopic");
            extFields.put("queueId", "0");
            extFields.put("bornTimestamp", "1700000000000");
            extFields.put("flag", "0");
            extFields.put("properties", "KEYS=key1\u0002TAGS=tagA");
            cmdWithBodyAndExtFields.setExtFields(extFields);
        }
    }

    @Benchmark
    @Threads(1)
    public void recordInbound_noBody_singleThread(HandlerState h, CommandState c) {
        h.handler.recordInbound(c.cmdNoBody);
    }

    @Benchmark
    @Threads(1)
    public void recordInbound_withBody_singleThread(HandlerState h, CommandState c) {
        h.handler.recordInbound(c.cmdWithBody);
    }

    /**
     * Key comparison: detailed-off vs detailed-on with 5 extFields.
     * Shows cost of O(n) extFields iteration when switch is enabled.
     */
    @Benchmark
    @Threads(1)
    public void recordInbound_withBodyAndExtFields_singleThread(HandlerState h, CommandState c) {
        h.handler.recordInbound(c.cmdWithBodyAndExtFields);
    }

    @Benchmark
    @Threads(4)
    public void recordInbound_withBody_4threads(HandlerState h, CommandState c) {
        h.handler.recordInbound(c.cmdWithBody);
    }

    @Benchmark
    @Threads(8)
    public void recordInbound_withBody_8threads(HandlerState h, CommandState c) {
        h.handler.recordInbound(c.cmdWithBody);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(RemotingCodeDistributionBenchmark.class.getSimpleName())
            .resultFormat(ResultFormatType.TEXT)
            .build();
        new Runner(opt).run();
    }
}
