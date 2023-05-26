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

package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStreamFactory;
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

@BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(2)
public class TieredFileSegmentInputStreamBenchmark {

    TieredFileSegmentInputStream inputStream;
    @Setup
    public void setUp() {
        List<ByteBuffer> bufferList = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            bufferList.add(ByteBuffer.allocateDirect(bufPerLen));
        }
        inputStream = TieredFileSegmentInputStreamFactory.build(
            TieredFileSegment.FileSegmentType.CONSUME_QUEUE, 0, bufferList, null, bufPerLen * 1000);
    }

    @Param({"512", "1024", "2048", "4096", "8192", "16384", "32768", "65536"})
    public int readLen;

    @Param({"512", "1024", "2048", "4096", "8192", "16384", "32768", "65536"})
    public int bufPerLen;

    @Benchmark
    public void benchNormalStream() throws Exception {
        int off = 0;
        while (off < bufPerLen * 1000) {
            byte[] buf = new byte[readLen];
            int len = inputStream.read(buf, off, readLen);
            if (len == -1) {
                break;
            }
            off += len;
        }
    }

    @Benchmark
    public void benchOptimizedStream() {
        int off = 0;
        while (off < bufPerLen * 1000) {
            byte[] buf = new byte[readLen];
            int len = inputStream.readOptimized(buf, off, readLen);
            if (len == -1) {
                break;
            }
            off += len;
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(TieredFileSegmentInputStreamBenchmark.class.getSimpleName())
            .resultFormat(ResultFormatType.JSON)
            .build();
        new Runner(opt).run();
    }
}
