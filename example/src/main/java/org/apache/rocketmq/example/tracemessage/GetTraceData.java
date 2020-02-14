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

package org.apache.rocketmq.example.tracemessage;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDataEncoder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.List;
import java.util.UUID;

import static org.apache.rocketmq.example.tracemessage.TraceProducer.TRACE_KEY;

/**
 * In this example we show you how to use message trace.
 * When the enableMsgTrace flag of producer/consumer is true,
 * trace data will be sent to RMQ_SYS_TRACE_TOPIC or customized topic(if provided),
 * so we can get message trace data by subscribe the topic.
 */
public class GetTraceData {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr("localhost:9876");
        mqAdminExt.start();
        // The param key should be original message's msgId or key
        QueryResult result = mqAdminExt.queryMessage(MixAll.RMQ_SYS_TRACE_TOPIC, TRACE_KEY, 32, 0, System.currentTimeMillis());
        List<MessageExt> msgs = result.getMessageList();

        for (int i = 0; i < msgs.size(); i++) {
            // Decode trace data
            List<TraceContext> traceContexts = TraceDataEncoder.decoderFromTraceDataString(new String(msgs.get(i).getBody()));
            for (TraceContext tc: traceContexts) {
                for (TraceBean tb: tc.getTraceBeans()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(msgs.get(i).getBornHostString()).append(" ")
                            .append(tc.getGroupName()).append(" ")
                            .append(tc.getTimeStamp()).append(" ")
                            .append(tc.getTraceType()).append(" ")
                            .append(tb.getTopic()).append(" ")
                            .append(tb.getMsgId()).append(" ")
                            .append(tb.getMsgType()).append(" ");
                    sb.append("%n");
                    System.out.printf(sb.toString());
                }
            }
        }
    }

}
