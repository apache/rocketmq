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
package org.apache.rocketmq.common.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MultipleAddrConvertor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static final int CACHE_MAX_LEN = 1024;

    private static LinkedHashMap<String, String> chosenAddrCache = new LinkedHashMap<String, String>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > CACHE_MAX_LEN;
        }
    };

    public static TopicRouteData convert(TopicRouteData topicRouteData) {
        if (topicRouteData == null) {
            return topicRouteData;
        }
        List<BrokerData> brokerDatas = topicRouteData.getBrokerDatas();

        for (BrokerData brokerData : brokerDatas) {
            convert(brokerData);
        }
        return topicRouteData;
    }

    public static BrokerData convert(BrokerData brokerData) {

        HashMap<Long, String> brokerIdAddr = brokerData.getBrokerAddrs();
        for (Long brokerId : brokerIdAddr.keySet()) {

            String multipleAddr = brokerIdAddr.get(brokerId);
            if (multipleAddr == null) {
            } else {
                String[] addrs = multipleAddr.split(";");
                if (addrs.length <= 1) {
                } else {
                    if (chosenAddrCache.get(multipleAddr) != null) {
                        brokerIdAddr.put(brokerId, chosenAddrCache.get(multipleAddr));
                    } else {
                        String addr = convert(multipleAddr);
                        if (addr != null) {
                            chosenAddrCache.put(multipleAddr, addr);
                        }
                        brokerIdAddr.put(brokerId, convert(addr));
                    }
                }
            }
        }

        return brokerData;
    }

    public static String convert(String multipleAddr) {

        if (multipleAddr == null || multipleAddr.length() < 8) {
            return multipleAddr;
        }
        String[] ipsPort = multipleAddr.split(":");
        if (ipsPort.length != 2) {
            return null;
        } else {
            try {
                Integer.parseInt(ipsPort[1]);
            } catch (Exception e) {
                return null;
            }
        }
        final String portNum = ipsPort[1];
        String[] ips = ipsPort[0].split(";");
        if (ips.length <= 1) {
            return multipleAddr;
        }

        final List<String> availableAddrs = new ArrayList<String>();

        for (final String aip : ips) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            executor.submit(new Runnable() {
                @Override public void run() {
                    try {
                        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName(aip), Integer.parseInt(portNum));
                        SocketChannel sc = SocketChannel.open();
                        sc.configureBlocking(true);

                        if (sc.connect(socketAddress)) {
                            availableAddrs.add(aip + ":" + portNum);
                            sc.close();
                        }

                    } catch (Exception e) {
                        log.info("Exception when host detecting " + aip + ":" + portNum);
                    }
                }
            });

            try {
                if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
                } else {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.info("Exception when waiting host detecting for " + aip + ":" + portNum);
                e.printStackTrace();
            }
        }

        if (availableAddrs.size() >= 1) {
            return availableAddrs.get(0);
        }
        return null;
    }
}
