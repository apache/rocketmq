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

package org.apache.rocketmq.client.producer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

public class RequestFutureHolder {
    private static InternalLogger log = ClientLogger.getLog();
    private static final RequestFutureHolder INSTANCE = new RequestFutureHolder();
    private ConcurrentHashMap<String, RequestResponseFuture> requestFutureTable = new ConcurrentHashMap<String, RequestResponseFuture>();
    private final AtomicInteger producerNum = new AtomicInteger(0);
    private ScheduledExecutorService scheduledExecutorService = null;

    public ConcurrentHashMap<String, RequestResponseFuture> getRequestFutureTable() {
        return requestFutureTable;
    }

    private void scanExpiredRequest() {
        final List<RequestResponseFuture> rfList = new LinkedList<RequestResponseFuture>();
        Iterator<Map.Entry<String, RequestResponseFuture>> it = requestFutureTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RequestResponseFuture> next = it.next();
            RequestResponseFuture rep = next.getValue();

            if (rep.isTimeout()) {
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, CorrelationId={}" + rep.getCorrelationId());
            }
        }

        for (RequestResponseFuture rf : rfList) {
            try {
                Throwable cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.");
                rf.setCause(cause);
                rf.executeRequestCallback();
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    public synchronized void startScheduledTask() {
        if (this.producerNum.incrementAndGet() == 1) {
            this.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        RequestFutureHolder.getInstance().scanExpiredRequest();
                    } catch (Throwable e) {
                        log.error("scan RequestFutureTable exception", e);
                    }
                }
            }, 1000 * 3, 1000, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void shutdown() {
        if (this.producerNum.decrementAndGet() == 0 && this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdown();
            this.scheduledExecutorService = null;
        }
    }

    private RequestFutureHolder() {
    }

    private ScheduledExecutorService getScheduledExecutorService() {
        if (null == scheduledExecutorService) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "RequestHouseKeepingService");
                }
            });
        }
        return scheduledExecutorService;
    }

    public static RequestFutureHolder getInstance() {
        return INSTANCE;
    }
}
