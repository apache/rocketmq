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

package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TraceDataEncoderTest {

    private String traceData;

    private long time;

    @Before
    public void init() {
        time = System.currentTimeMillis();
        traceData = new StringBuilder()
            .append("Pub").append(TraceConstants.CONTENT_SPLITOR)
            .append(time).append(TraceConstants.CONTENT_SPLITOR)
            .append("DefaultRegion").append(TraceConstants.CONTENT_SPLITOR)
            .append("PID-test").append(TraceConstants.CONTENT_SPLITOR)
            .append("topic-test").append(TraceConstants.CONTENT_SPLITOR)
            .append("AC1415116D1418B4AAC217FE1B4E0000").append(TraceConstants.CONTENT_SPLITOR)
            .append("Tags").append(TraceConstants.CONTENT_SPLITOR)
            .append("Keys").append(TraceConstants.CONTENT_SPLITOR)
            .append("127.0.0.1:10911").append(TraceConstants.CONTENT_SPLITOR)
            .append(26).append(TraceConstants.CONTENT_SPLITOR)
            .append(245).append(TraceConstants.CONTENT_SPLITOR)
            .append(MessageType.Normal_Msg.ordinal()).append(TraceConstants.CONTENT_SPLITOR)
            .append("0A9A002600002A9F0000000000002329").append(TraceConstants.CONTENT_SPLITOR)
            .append(true).append(TraceConstants.CONTENT_SPLITOR)
            .append(UtilAll.ipToIPv4Str(UtilAll.getIP())).append(TraceConstants.FIELD_SPLITOR)
            .toString();
    }

    @Test
    public void testDecoderFromTraceDataString() {
        List<TraceContext> contexts = TraceDataEncoder.decoderFromTraceDataString(traceData);
        Assert.assertEquals(contexts.size(), 1);
        Assert.assertEquals(contexts.get(0).getTraceType(), TraceType.Pub);
    }


    @Test
    public void testEncoderFromContextBean() {
        TraceContext context = new TraceContext();
        context.setTraceType(TraceType.Pub);
        context.setGroupName("PID-test");
        context.setRegionId("DefaultRegion");
        context.setCostTime(245);
        context.setSuccess(true);
        context.setTimeStamp(time);
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic("topic-test");
        traceBean.setKeys("Keys");
        traceBean.setTags("Tags");
        traceBean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        traceBean.setOffsetMsgId("0A9A002600002A9F0000000000002329");
        traceBean.setStoreHost("127.0.0.1:10911");
        traceBean.setStoreTime(time);
        traceBean.setMsgType(MessageType.Normal_Msg);
        traceBean.setBodyLength(26);
        List<TraceBean> traceBeans = new ArrayList<TraceBean>();
        traceBeans.add(traceBean);
        context.setTraceBeans(traceBeans);
        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(context);

        Assert.assertEquals(traceTransferBean.getTransData(), traceData);
        Assert.assertEquals(traceTransferBean.getTransKey().size(), 2);
    }

}