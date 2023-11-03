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

package org.apache.rocketmq.tieredstore.index;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.file.TieredFileAllocator;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.Assert;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Djava.net.preferIPv4Stack=true", "-Djmh.rmi.port=1099"})
public class IndexStoreServiceBenchTest {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private static final String TOPIC_NAME = "TopicTest";
    private TieredMessageStoreConfig storeConfig;
    private IndexStoreService indexStoreService;
    private final LongAdder failureCount = new LongAdder();

    @Setup
    public void init() throws ClassNotFoundException, NoSuchMethodException {
        String storePath = Paths.get(System.getProperty("user.home"), "store_test", "index").toString();
        UtilAll.deleteFile(new File(storePath));
        UtilAll.deleteFile(new File("./e96d41b2_IndexService"));
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerClusterName("IndexService");
        storeConfig.setBrokerName("IndexServiceBroker");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(500 * 1000);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(2000 * 1000);
        TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();
        TieredFileAllocator tieredFileAllocator = new TieredFileAllocator(storeConfig);
        indexStoreService = new IndexStoreService(tieredFileAllocator, storePath);
        indexStoreService.start();
    }

    @TearDown
    public void shutdown() throws IOException {
        indexStoreService.shutdown();
        indexStoreService.destroy();
        TieredStoreExecutor.shutdown();
    }

    //@Benchmark
    @Threads(2)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 1)
    public void doPutThroughputBenchmark() {
        for (int i = 0; i < 100; i++) {
            AppendResult result = indexStoreService.putKey(
                TOPIC_NAME, 123, 2, Collections.singleton(String.valueOf(i)),
                i * 100L, i * 100, System.currentTimeMillis());
            if (AppendResult.SUCCESS.equals(result)) {
                failureCount.increment();
            }
        }
    }

    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 1)
    public void doGetThroughputBenchmark() throws ExecutionException, InterruptedException {
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < storeConfig.getTieredStoreIndexFileMaxIndexNum(); i++) {
                indexStoreService.putKey(
                    "TopicTest", 123, j, Collections.singleton(String.valueOf(i)),
                    i * 100L, i * 100, System.currentTimeMillis());
            }
        }

        int queryCount = 100 * 10000;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < queryCount; i++) {
            List<IndexItem> indexItems = indexStoreService.queryAsync(TOPIC_NAME, String.valueOf(i),
                20, 0, System.currentTimeMillis()).get();
            Assert.assertEquals(10, indexItems.size());

            List<IndexItem> indexItems2 = indexStoreService.queryAsync(TOPIC_NAME, String.valueOf(i),
                5, 0, System.currentTimeMillis()).get();
            Assert.assertEquals(5, indexItems2.size());
        }
        log.info("DoGetThroughputBenchmark test cost: {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(IndexStoreServiceBenchTest.class.getSimpleName())
            .warmupIterations(0)
            .measurementIterations(1)
            .result("result.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
        new Runner(opt).run();
    }
}
