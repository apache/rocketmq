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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DuplicateMessageInfo<T> {

    public void checkDuplicatedMessageInfo(boolean bPrintLog,
        List<List<T>> lQueueList) throws IOException {
        int msgListSize = lQueueList.size();
        int maxmsgList = 0;
        Map<T, Integer> msgIdMap = new HashMap<T, Integer>();
        Map<Integer, Integer> dupMsgMap = new HashMap<Integer, Integer>();

        for (int i = 0; i < msgListSize; i++) {
            if (maxmsgList < lQueueList.get(i).size())
                maxmsgList = lQueueList.get(i).size();
        }

        List<StringBuilder> strBQueue = new LinkedList<StringBuilder>();
        for (int i = 0; i < msgListSize; i++)
            strBQueue.add(new StringBuilder());

        for (int msgListIndex = 0; msgListIndex < maxmsgList; msgListIndex++) {
            for (int msgQueueListIndex = 0; msgQueueListIndex < msgListSize; msgQueueListIndex++) {
                if (msgListIndex < lQueueList.get(msgQueueListIndex).size()) {
                    if (msgIdMap.containsKey(lQueueList.get(msgQueueListIndex).get(msgListIndex))) {
                        if (dupMsgMap.containsKey(msgQueueListIndex)) {
                            int dupMsgCount = dupMsgMap.get(msgQueueListIndex);
                            dupMsgCount++;
                            dupMsgMap.remove(msgQueueListIndex);
                            dupMsgMap.put(msgQueueListIndex, dupMsgCount);
                        } else {
                            dupMsgMap.put(msgQueueListIndex, 1);
                        }

                        strBQueue.get(msgQueueListIndex).append("" + msgQueueListIndex + "\t" +
                            msgIdMap.get(lQueueList.get(msgQueueListIndex).get(msgListIndex)) + "\t"
                            + lQueueList.get(msgQueueListIndex).get(msgListIndex) + "\r\n");
                    } else {
                        msgIdMap.put(lQueueList.get(msgQueueListIndex).get(msgListIndex), msgQueueListIndex);
                    }
                }
            }
        }

        int msgTotalNum = getMsgTotalNumber(lQueueList);
        int msgTotalDupNum = getDuplicateMsgNum(dupMsgMap);
        int msgNoDupNum = msgTotalNum - msgTotalDupNum;
        float msgDupRate = ((float) msgTotalDupNum / (float) msgTotalNum) * 100.0f;
        StringBuilder strBuilder = new StringBuilder();

        strBuilder.append("msgTotalNum:" + msgTotalNum + "\r\n");
        strBuilder.append("msgTotalDupNum:" + msgTotalDupNum + "\r\n");
        strBuilder.append("msgNoDupNum:" + msgNoDupNum + "\r\n");
        strBuilder.append("msgDupRate" + getFloatNumString(msgDupRate) + "%\r\n");

        strBuilder.append("queue\tmsg(dupNum/dupRate)\tdupRate\r\n");
        for (int i = 0; i < dupMsgMap.size(); i++) {
            int msgDupNum = dupMsgMap.get(i);
            int msgNum = lQueueList.get(i).size();
            float msgQueueDupRate = ((float) msgDupNum / (float) msgTotalDupNum) * 100.0f;
            float msgQueueInnerDupRate = ((float) msgDupNum / (float) msgNum) * 100.0f;

            strBuilder.append(i + "\t" + msgDupNum + "/" + getFloatNumString(msgQueueDupRate) + "%" + "\t\t" +
                getFloatNumString(msgQueueInnerDupRate) + "%\r\n");
        }

        System.out.print(strBuilder.toString());
        String titleString = "queue\tdupQueue\tdupMsg\r\n";
        System.out.print(titleString);

        for (int i = 0; i < msgListSize; i++)
            System.out.print(strBQueue.get(i).toString());

        if (bPrintLog) {
            String logFileNameStr = "D:" + File.separator + "checkDuplicatedMessageInfo.txt";
            File logFileNameFile = new File(logFileNameStr);
            OutputStream out = new FileOutputStream(logFileNameFile, true);

            String strToWrite;
            byte[] byteToWrite;
            strToWrite = strBuilder.toString() + titleString;
            for (int i = 0; i < msgListSize; i++)
                strToWrite += strBQueue.get(i).toString() + "\r\n";

            byteToWrite = strToWrite.getBytes();
            out.write(byteToWrite);
            out.close();
        }
    }

    private int getMsgTotalNumber(List<List<T>> lQueueList) {
        int msgTotalNum = 0;
        for (int i = 0; i < lQueueList.size(); i++) {
            msgTotalNum += lQueueList.get(i).size();
        }
        return msgTotalNum;
    }

    private int getDuplicateMsgNum(Map<Integer, Integer> msgDupMap) {
        int msgDupNum = 0;
        for (int i = 0; i < msgDupMap.size(); i++) {
            msgDupNum += msgDupMap.get(i);
        }
        return msgDupNum;
    }

    private String getFloatNumString(float fNum) {
        DecimalFormat dcmFmt = new DecimalFormat("0.00");
        return dcmFmt.format(fNum);
    }
}
