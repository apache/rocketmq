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

package org.apache.rocketmq.grpc.channel;

import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcClientChannelManager {
    public static Logger log = LoggerFactory.getLogger(GrpcClientChannelManager.class);

    private static final String MID_SEPARATOR = "%%";

    private static final long CLIENT_OBSERVER_EXPIRED_MILLIS = 30 * 1000;

    private static final long CHECK_EXPIRED_INTERVAL_MILLIS = 30 * 1000;

    /**
     * Client stream observer
     * group, clientId -> client channel
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, GrpcClientObserver>> clientChannels
        = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService =
        new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("GrpcClientChannelsThread_"));

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            AtomicLong remainCount = new AtomicLong();
            AtomicLong clearCount = new AtomicLong();

            clientChannels.forEach((group, channels) -> {
                channels.forEach((clientId, grpcClientObserver) -> {
                    if (now - grpcClientObserver.getLastUseTimestamp() > CLIENT_OBSERVER_EXPIRED_MILLIS) {
                        this.remove(group, clientId);
                        clearCount.getAndIncrement();
                    } else {
                        remainCount.getAndIncrement();
                    }
                });
            });

            log.info("grpc client channels clear. clearCount: {}, remainCount: {}", clearCount.get(), remainCount.get());
        }, 0, CHECK_EXPIRED_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Add a connection
     *
     * @param group          consumer/producer group
     * @param clientId       client id
     * @param streamObserver gRPC stream observer
     */
    public synchronized void add(String group, String clientId, MultiplexingRequest request,
        StreamObserver<MultiplexingResponse> streamObserver) {
        ConcurrentHashMap<String, GrpcClientObserver> channels = this.clientChannels.get(group);

        if (channels == null) {
            channels = new ConcurrentHashMap<>(1);
            this.clientChannels.put(group, channels);
        }

        GrpcClientObserver channel = channels.get(clientId);
        if (channel == null) {
            channel = new GrpcClientObserver(group, clientId);
            channels.put(clientId, channel);
        }

        channel.setClientObserver(request, streamObserver);
    }

    /**
     * Remove a connection
     *
     * @param group    consumer/producer group
     * @param clientId client id
     */
    public synchronized GrpcClientObserver remove(String group, String clientId) {
        ConcurrentHashMap<String, GrpcClientObserver> channels = this.clientChannels.get(group);
        if (channels == null) {
            return null;
        }

        GrpcClientObserver clientChannel = channels.remove(clientId);
        if (channels.isEmpty()) {
            this.clientChannels.remove(group);
        }

        return clientChannel;
    }

    /**
     * Get connection by client id
     *
     * @param group    consumer/producer group
     * @param clientId client id
     * @return gRPC client channel
     */
    public GrpcClientObserver get(String group, String clientId) {
        ConcurrentHashMap<String, GrpcClientObserver> channels = clientChannels.get(group);
        if (channels == null) {
            return null;
        }

        return channels.get(clientId);
    }

    /**
     * Get all the client id by group
     *
     * @param group consumer/producer group
     * @return client id list
     */
    public List<String> getClientIds(String group) {
        ConcurrentHashMap<String, GrpcClientObserver> channels = clientChannels.get(group);
        if (channels == null) {
            return new ArrayList<>();
        }

        return new ArrayList<>(channels.keySet());
    }

    public GrpcClientObserver getByMid(String mid) {
        String[] groupAndClientId = mid.split(MID_SEPARATOR, 2);
        if (groupAndClientId.length < 2) {
            return null;
        }

        return this.get(groupAndClientId[0], groupAndClientId[1]);
    }

    /**
     * Build mid
     *
     * @param group    consumer group
     * @param clientId clientId
     * @return eg: GID_client%%clientId
     */
    public static String buildMid(String group, String clientId) {
        return group + MID_SEPARATOR + clientId;
    }
}
