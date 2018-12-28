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

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.namesrv.TopAddressing;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.RPCHook;

@Deprecated
public class TraceProducerFactory {

    private static Map<String, Object> dispatcherTable = new ConcurrentHashMap<String, Object>();
    private static AtomicBoolean isStarted = new AtomicBoolean(false);
    private static DefaultMQProducer traceProducer;


    public static DefaultMQProducer getTraceDispatcherProducer(Properties properties, RPCHook rpcHook) {
        if (traceProducer == null) {

            traceProducer = new DefaultMQProducer(rpcHook);
            traceProducer.setProducerGroup(TraceConstants.GROUP_NAME);
            traceProducer.setSendMsgTimeout(5000);
            traceProducer.setInstanceName(properties.getProperty(TraceConstants.INSTANCE_NAME, String.valueOf(System.currentTimeMillis())));
            String nameSrv = properties.getProperty(TraceConstants.NAMESRV_ADDR);
            if (nameSrv == null) {
                TopAddressing topAddressing = new TopAddressing(properties.getProperty(TraceConstants.ADDRSRV_URL));
                nameSrv = topAddressing.fetchNSAddr();
            }
            traceProducer.setNamesrvAddr(nameSrv);
            traceProducer.setVipChannelEnabled(false);
            //the max size of message is 128K
            int maxSize = Integer.parseInt(properties.getProperty(TraceConstants.MAX_MSG_SIZE, "128000"));
            traceProducer.setMaxMessageSize(maxSize - 10 * 1000);
        }
        return traceProducer;
    }

    public static void registerTraceDispatcher(String dispatcherId, String nameSrvAddr) throws MQClientException {
        dispatcherTable.put(dispatcherId, new Object());
        if (traceProducer != null && isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.start();
        }
    }

    public static void unregisterTraceDispatcher(String dispatcherId) {
        dispatcherTable.remove(dispatcherId);
        if (dispatcherTable.isEmpty() && traceProducer != null && isStarted.get()) {
            traceProducer.shutdown();
        }
    }
}
