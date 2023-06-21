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

package org.apache.rocketmq.remoting.protocol.body;

import com.alibaba.fastjson.JSON;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

public class RegisterBrokerBody extends RemotingSerializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
    private List<String> filterServerList = new ArrayList<>();
    private static final long MINIMUM_TAKE_TIME_MILLISECOND = 50;

    public byte[] encode(boolean compress) {

        if (!compress) {
            return super.encode();
        }
        long start = System.currentTimeMillis();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DeflaterOutputStream outputStream = new DeflaterOutputStream(byteArrayOutputStream, new Deflater(Deflater.BEST_COMPRESSION));
        DataVersion dataVersion = topicConfigSerializeWrapper.getDataVersion();
        ConcurrentMap<String, TopicConfig> topicConfigTable = cloneTopicConfigTable(topicConfigSerializeWrapper.getTopicConfigTable());
        assert topicConfigTable != null;
        try {
            byte[] buffer = dataVersion.encode();

            // write data version
            outputStream.write(convertIntToByteArray(buffer.length));
            outputStream.write(buffer);

            int topicNumber = topicConfigTable.size();

            // write number of topic configs
            outputStream.write(convertIntToByteArray(topicNumber));

            // write topic config entry one by one.
            for (ConcurrentMap.Entry<String, TopicConfig> next : topicConfigTable.entrySet()) {
                buffer = next.getValue().encode().getBytes(MixAll.DEFAULT_CHARSET);
                outputStream.write(convertIntToByteArray(buffer.length));
                outputStream.write(buffer);
            }

            buffer = JSON.toJSONString(filterServerList).getBytes(MixAll.DEFAULT_CHARSET);

            // write filter server list json length
            outputStream.write(convertIntToByteArray(buffer.length));

            // write filter server list json
            outputStream.write(buffer);

            //write the topic queue mapping
            Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigSerializeWrapper.getTopicQueueMappingInfoMap();
            if (topicQueueMappingInfoMap == null) {
                //as the placeholder
                topicQueueMappingInfoMap = new ConcurrentHashMap<>();
            }
            outputStream.write(convertIntToByteArray(topicQueueMappingInfoMap.size()));
            for (TopicQueueMappingInfo info: topicQueueMappingInfoMap.values()) {
                buffer = JSON.toJSONString(info).getBytes(MixAll.DEFAULT_CHARSET);
                outputStream.write(convertIntToByteArray(buffer.length));
                // write filter server list json
                outputStream.write(buffer);
            }

            outputStream.finish();
            long takeTime = System.currentTimeMillis() - start;
            if (takeTime > MINIMUM_TAKE_TIME_MILLISECOND) {
                LOGGER.info("Compressing takes {}ms", takeTime);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Failed to compress RegisterBrokerBody object", e);
        }

        return null;
    }

    public static RegisterBrokerBody decode(byte[] data, boolean compressed, MQVersion.Version brokerVersion) throws IOException {
        if (!compressed) {
            return RegisterBrokerBody.decode(data, RegisterBrokerBody.class);
        }
        long start = System.currentTimeMillis();
        InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(data));
        int dataVersionLength = readInt(inflaterInputStream);
        byte[] dataVersionBytes = readBytes(inflaterInputStream, dataVersionLength);
        DataVersion dataVersion = DataVersion.decode(dataVersionBytes, DataVersion.class);

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        registerBrokerBody.getTopicConfigSerializeWrapper().setDataVersion(dataVersion);
        ConcurrentMap<String, TopicConfig> topicConfigTable = registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable();

        int topicConfigNumber = readInt(inflaterInputStream);
        LOGGER.debug("{} topic configs to extract", topicConfigNumber);

        for (int i = 0; i < topicConfigNumber; i++) {
            int topicConfigJsonLength = readInt(inflaterInputStream);

            byte[] buffer = readBytes(inflaterInputStream, topicConfigJsonLength);
            TopicConfig topicConfig = new TopicConfig();
            String topicConfigJson = new String(buffer, MixAll.DEFAULT_CHARSET);
            topicConfig.decode(topicConfigJson);
            topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        int filterServerListJsonLength = readInt(inflaterInputStream);

        byte[] filterServerListBuffer = readBytes(inflaterInputStream, filterServerListJsonLength);
        String filterServerListJson = new String(filterServerListBuffer, MixAll.DEFAULT_CHARSET);
        List<String> filterServerList = new ArrayList<>();
        try {
            filterServerList = JSON.parseArray(filterServerListJson, String.class);
        } catch (Exception e) {
            LOGGER.error("Decompressing occur Exception {}", filterServerListJson);
        }

        registerBrokerBody.setFilterServerList(filterServerList);

        if (brokerVersion.ordinal() >= MQVersion.Version.V5_0_0.ordinal()) {
            int topicQueueMappingNum = readInt(inflaterInputStream);
            Map<String/* topic */, TopicQueueMappingInfo> topicQueueMappingInfoMap = new ConcurrentHashMap<>();
            for (int i = 0; i < topicQueueMappingNum; i++) {
                int mappingJsonLen = readInt(inflaterInputStream);
                byte[] buffer = readBytes(inflaterInputStream, mappingJsonLen);
                TopicQueueMappingInfo info = TopicQueueMappingInfo.decode(buffer, TopicQueueMappingInfo.class);
                topicQueueMappingInfoMap.put(info.getTopic(), info);
            }
            registerBrokerBody.getTopicConfigSerializeWrapper().setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        long takeTime = System.currentTimeMillis() - start;
        if (takeTime > MINIMUM_TAKE_TIME_MILLISECOND) {
            LOGGER.info("Decompressing takes {}ms", takeTime);
        }
        return registerBrokerBody;
    }

    private static byte[] convertIntToByteArray(int n) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(n);
        return byteBuffer.array();
    }

    private static byte[] readBytes(InflaterInputStream inflaterInputStream, int length) throws IOException {
        byte[] buffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int len = inflaterInputStream.read(buffer, bytesRead, length - bytesRead);
            if (len == -1) {
                throw new IOException("End of compressed data has reached");
            } else {
                bytesRead += len;
            }
        }
        return buffer;
    }

    private static int readInt(InflaterInputStream inflaterInputStream) throws IOException {
        byte[] buffer = readBytes(inflaterInputStream, 4);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        return byteBuffer.getInt();
    }

    public TopicConfigAndMappingSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public void setTopicConfigSerializeWrapper(TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }

    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }

    private ConcurrentMap<String, TopicConfig> cloneTopicConfigTable(
        ConcurrentMap<String, TopicConfig> topicConfigConcurrentMap) {
        if (topicConfigConcurrentMap == null) {
            return null;
        }
        ConcurrentHashMap<String, TopicConfig> result = new ConcurrentHashMap<>(topicConfigConcurrentMap.size());
        result.putAll(topicConfigConcurrentMap);
        return result;
    }
}
