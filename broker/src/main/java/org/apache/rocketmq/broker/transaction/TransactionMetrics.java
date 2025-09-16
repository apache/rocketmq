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
package org.apache.rocketmq.broker.transaction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TransactionMetrics extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private ConcurrentMap<String, Metric> transactionCounts =
            new ConcurrentHashMap<>(1024);

    private DataVersion dataVersion = new DataVersion();

    private final String configPath;

    public TransactionMetrics(String configPath) {
        this.configPath = configPath;
    }

    public long addAndGet(String topic, int value) {
        Metric pair = getTopicPair(topic);
        getDataVersion().nextVersion();
        pair.setTimeStamp(System.currentTimeMillis());
        return pair.getCount().addAndGet(value);
    }

    public Metric getTopicPair(String topic) {
        Metric pair = transactionCounts.get(topic);
        if (null != pair) {
            return pair;
        }
        pair = new Metric();
        final Metric previous = transactionCounts.putIfAbsent(topic, pair);
        if (null != previous) {
            return previous;
        }
        return pair;
    }
    public long getTransactionCount(String topic) {
        Metric pair = transactionCounts.get(topic);
        if (null == pair) {
            return 0;
        } else {
            return pair.getCount().get();
        }
    }

    public Map<String, Metric> getTransactionCounts() {
        return transactionCounts;
    }
    public void setTransactionCounts(ConcurrentMap<String, Metric> transactionCounts) {
        this.transactionCounts = transactionCounts;
    }

    protected void write0(Writer writer) {
        TransactionMetricsSerializeWrapper wrapper = new TransactionMetricsSerializeWrapper();
        wrapper.setTransactionCount(transactionCounts);
        wrapper.setDataVersion(dataVersion);
        JSON.writeJSONString(writer, wrapper, SerializerFeature.BrowserCompatible);
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return configPath;
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TransactionMetricsSerializeWrapper transactionMetricsSerializeWrapper =
                    TransactionMetricsSerializeWrapper.fromJson(jsonString, TransactionMetricsSerializeWrapper.class);
            if (transactionMetricsSerializeWrapper != null) {
                this.transactionCounts.putAll(transactionMetricsSerializeWrapper.getTransactionCount());
                this.dataVersion.assignNewOne(transactionMetricsSerializeWrapper.getDataVersion());
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        TransactionMetricsSerializeWrapper metricsSerializeWrapper = new TransactionMetricsSerializeWrapper();
        metricsSerializeWrapper.setDataVersion(this.dataVersion);
        metricsSerializeWrapper.setTransactionCount(this.transactionCounts);
        return metricsSerializeWrapper.toJson(prettyFormat);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public void cleanMetrics(Set<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<String, Metric>> iterator = transactionCounts.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Metric> entry = iterator.next();
            final String topic = entry.getKey();
            if (topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)) {
                continue;
            }
            if (!topics.contains(topic)) {
                continue;
            }
            // in the input topics set, then remove it.
            iterator.remove();
        }
    }

    public static class TransactionMetricsSerializeWrapper extends RemotingSerializable {
        private ConcurrentMap<String, Metric> transactionCount =
                new ConcurrentHashMap<>(1024);
        private DataVersion dataVersion = new DataVersion();

        public ConcurrentMap<String, Metric> getTransactionCount() {
            return transactionCount;
        }

        public void setTransactionCount(
                ConcurrentMap<String, Metric> transactionCount) {
            this.transactionCount = transactionCount;
        }

        public DataVersion getDataVersion() {
            return dataVersion;
        }

        public void setDataVersion(DataVersion dataVersion) {
            this.dataVersion = dataVersion;
        }
    }

    @Override
    public synchronized void persist() {
        try {
            // bak metrics file
            String config = configFilePath();
            String backup = config + ".bak";
            File configFile = new File(config);
            File bakFile = new File(backup);

            if (configFile.exists()) {
                // atomic move
                Files.move(configFile.toPath(), bakFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

                // sync the directory, ensure that the bak file is visible
                MixAll.fsyncDirectory(Paths.get(bakFile.getParent()));
            }

            File dir = new File(Path.of(config).getParent().toString());
            if (!dir.exists()) {
                Files.createDirectories(dir.toPath());
            }

            // persist metrics file
            StringWriter stringWriter = new StringWriter();
            write0(stringWriter);
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(config, "rw")) {
                randomAccessFile.write(stringWriter.toString().getBytes(StandardCharsets.UTF_8));
                randomAccessFile.getChannel().force(true);
                // sync the directory, ensure that the config file is visible
                MixAll.fsyncDirectory(Paths.get(configFile.getParent()));
            }
        } catch (Throwable t) {
            log.error("Failed to persist", t);
        }
    }

    public static class Metric {
        private AtomicLong count;
        private long timeStamp;

        public Metric() {
            count = new AtomicLong(0);
            timeStamp = System.currentTimeMillis();
        }

        public AtomicLong getCount() {
            return count;
        }

        public void setCount(AtomicLong count) {
            this.count = count;
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return String.format("[%d,%d]", count.get(), timeStamp);
        }
    }

}
