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
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TransactionMetrics extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ConcurrentMap<String, Metric> transactionCounts =
            new ConcurrentHashMap<>(1024);

    private final DataVersion dataVersion = new DataVersion();

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

    protected void write0(Writer writer) {
        TransactionMetricsSerializeWrapper wrapper = new TransactionMetricsSerializeWrapper();
        wrapper.setTimingCount(transactionCounts);
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
                this.transactionCounts.putAll(transactionMetricsSerializeWrapper.getTimingCount());
                this.dataVersion.assignNewOne(transactionMetricsSerializeWrapper.getDataVersion());
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        TransactionMetricsSerializeWrapper metricsSerializeWrapper = new TransactionMetricsSerializeWrapper();
        metricsSerializeWrapper.setDataVersion(this.dataVersion);
        metricsSerializeWrapper.setTimingCount(this.transactionCounts);
        return metricsSerializeWrapper.toJson(prettyFormat);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
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
            if (topics.contains(topic)) {
                continue;
            }

            iterator.remove();
            log.info("clean timer metrics, because not in topic config, {}", topic);
        }
    }

    public static class TransactionMetricsSerializeWrapper extends RemotingSerializable {
        private ConcurrentMap<String, Metric> transactionCount =
                new ConcurrentHashMap<>(1024);
        private DataVersion dataVersion = new DataVersion();

        public ConcurrentMap<String, Metric> getTimingCount() {
            return transactionCount;
        }

        public void setTimingCount(
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
        String config = configFilePath();
        String temp = config + ".tmp";
        String backup = config + ".bak";
        BufferedWriter bufferedWriter = null;
        try {
            File tmpFile = new File(temp);
            File parentDirectory = tmpFile.getParentFile();
            if (!parentDirectory.exists()) {
                if (!parentDirectory.mkdirs()) {
                    log.error("Failed to create directory: {}", parentDirectory.getCanonicalPath());
                    return;
                }
            }

            if (!tmpFile.exists()) {
                if (!tmpFile.createNewFile()) {
                    log.error("Failed to create file: {}", tmpFile.getCanonicalPath());
                    return;
                }
            }
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile, false),
                    StandardCharsets.UTF_8));
            write0(bufferedWriter);
            bufferedWriter.flush();
            bufferedWriter.close();
            log.debug("Finished writing tmp file: {}", temp);

            File configFile = new File(config);
            if (configFile.exists()) {
                Files.copy(configFile, new File(backup));
                configFile.delete();
            }

            tmpFile.renameTo(configFile);
        } catch (IOException e) {
            log.error("Failed to persist {}", temp, e);
        } finally {
            if (null != bufferedWriter) {
                try {
                    bufferedWriter.close();
                } catch (IOException ignore) {
                }
            }
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
