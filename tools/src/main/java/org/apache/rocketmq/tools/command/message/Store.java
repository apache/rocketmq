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

package org.apache.rocketmq.tools.command.message;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class Store {
    public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;
    private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    private MappedFileQueue mapedFileQueue;
    private ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    private String cStorePath;
    private int cSize;
    private String lStorePath;
    private int lSize;

    public Store(String cStorePath, int cSize, String lStorePath, int lSize) {
        this.cStorePath = cStorePath;
        this.cSize = cSize;
        this.lStorePath = lStorePath;
        this.lSize = lSize;
        mapedFileQueue = new MappedFileQueue(cStorePath, cSize, null);
        consumeQueueTable =
            new ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>>();
    }

    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        System.out.printf("load commit log " + (result ? "OK" : "Failed"));
        if (result) {
            result = loadConsumeQueue();
        }
        System.out.printf("load logics log " + (result ? "OK" : "Failed"));
        return result;
    }

    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(lStorePath));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId = Integer.parseInt(fileQueueId.getName());
                        ConsumeQueue logic = new ConsumeQueue(
                            topic,
                            queueId,
                            StorePathConfigHelper.getStorePathConsumeQueue(lStorePath),
                            lSize,
                            null);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }
        System.out.printf("load logics queue all over, OK");
        return true;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    public void traval(boolean openAll) {
        boolean success = true;
        byte[] bytesContent = new byte[1024];
        List<MappedFile> mapedFiles = this.mapedFileQueue.getMappedFiles();
        ALL:
        for (MappedFile mapedFile : mapedFiles) {
            long startOffset = mapedFile.getFileFromOffset();
            int position = 0;
            int msgCount = 0;
            int errorCount = 0;

            System.out.printf("start travel " + mapedFile.getFileName());
            long startTime = System.currentTimeMillis();
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            while (byteBuffer.hasRemaining()) {
                // 1 TOTALSIZE
                int totalSize = byteBuffer.getInt();
                // 2 MAGICCODE
                int magicCode = byteBuffer.getInt();
                if (BLANK_MAGIC_CODE == magicCode) {
                    position = byteBuffer.limit();
                    break;
                }
                // 3 BODYCRC
                int bodyCRC = byteBuffer.getInt();

                // 4 QUEUEID
                int queueId = byteBuffer.getInt();

                // 5 FLAG
                int flag = byteBuffer.getInt();

                // 6 QUEUEOFFSET
                long queueOffset = byteBuffer.getLong();

                // 7 PHYSICALOFFSET
                long physicOffset = byteBuffer.getLong();

                // 8 SYSFLAG
                int sysFlag = byteBuffer.getInt();

                // 9 BORNTIMESTAMP
                long bornTimeStamp = byteBuffer.getLong();

                // 10 BORNHOST(IP+PORT)
                byteBuffer.position(byteBuffer.position() + 8);

                // 11 STORETIMESTAMP
                long storeTimestamp = byteBuffer.getLong();

                // 12 STOREHOST(IP+PORT)
                byteBuffer.position(byteBuffer.position() + 8);

                // 13 RECONSUMETIMES
                int reconsumeTimes = byteBuffer.getInt();

                // 14 Prepared Transaction Offset
                long preparedTransactionOffset = byteBuffer.getLong();

                // 15 BODY
                int bodyLen = byteBuffer.getInt();
                if (bodyLen > 0) {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }

                // 16 TOPIC
                byte topicLen = byteBuffer.get();
                byteBuffer.get(bytesContent, 0, topicLen);
                String topic = null;
                try {
                    topic = new String(bytesContent, 0, topicLen, MixAll.DEFAULT_CHARSET);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                Date storeTime = new Date(storeTimestamp);

                long currentPhyOffset = startOffset + position;
                if (physicOffset != currentPhyOffset) {
                    System.out.printf(storeTime
                        + " [fetal error] physicOffset != currentPhyOffset. position=" + position
                        + ", msgCount=" + msgCount + ", physicOffset=" + physicOffset
                        + ", currentPhyOffset=" + currentPhyOffset);
                    errorCount++;
                    if (!openAll) {
                        success = false;
                        break ALL;
                    }
                }

                ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
                SelectMappedBufferResult smb = consumeQueue.getIndexBuffer(queueOffset);
                try {
                    long offsetPy = smb.getByteBuffer().getLong();
                    int sizePy = smb.getByteBuffer().getInt();
                    if (physicOffset != offsetPy) {
                        System.out.printf(storeTime + " [fetal error] physicOffset != offsetPy. position="
                            + position + ", msgCount=" + msgCount + ", physicOffset=" + physicOffset
                            + ", offsetPy=" + offsetPy);
                        errorCount++;
                        if (!openAll) {
                            success = false;
                            break ALL;
                        }
                    }
                    if (totalSize != sizePy) {
                        System.out.printf(storeTime + " [fetal error] totalSize != sizePy. position="
                            + position + ", msgCount=" + msgCount + ", totalSize=" + totalSize
                            + ", sizePy=" + sizePy);
                        errorCount++;
                        if (!openAll) {
                            success = false;
                            break ALL;
                        }
                    }
                } finally {
                    smb.release();
                }

                msgCount++;
                position += totalSize;
                byteBuffer.position(position);
            }

            System.out.printf("end travel " + mapedFile.getFileName() + ", total msg=" + msgCount
                + ", error count=" + errorCount + ", cost:" + (System.currentTimeMillis() - startTime));
        }

        System.out.printf("travel " + (success ? "ok" : "fail"));
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap =
                new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }
        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueue(lStorePath),
                lSize,
                null);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }
        return logic;
    }
}