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

package org.apache.rocketmq.rpc.impl.config;

import org.apache.rocketmq.remoting.config.RemotingConfig;

public class RpcCommonConfig extends RemotingConfig {
    public final static String SERVICE_INVOKE_TIMEOUT = "service.invoke.timeout";
    public final static String SERVICE_THREAD_KEEP_ALIVE_TIME = "service.thread.keep.alive.time";
    private final static String SERVICE_ADDRESS_CACHE_TIME = "service.address.cache.time";
    private final static String SERVICE_CACHE_MAX_COUNT = "service.cache.max.count";
    private long serviceInvokeTimeout = 10000;
    private long serviceThreadKeepAliveTime = 60000;
    private long serviceAddressCacheTime = 30;
    private long serviceCacheMaxCount = 20000;

    public long getServiceInvokeTimeout() {
        return serviceInvokeTimeout;
    }

    public void setServiceInvokeTimeout(final long serviceInvokeTimeout) {
        this.serviceInvokeTimeout = serviceInvokeTimeout;
    }

    public long getServiceThreadKeepAliveTime() {
        return serviceThreadKeepAliveTime;
    }

    public void setServiceThreadKeepAliveTime(final long serviceThreadKeepAliveTime) {
        this.serviceThreadKeepAliveTime = serviceThreadKeepAliveTime;
    }

    public long getServiceAddressCacheTime() {
        return serviceAddressCacheTime;
    }

    public void setServiceAddressCacheTime(long serviceAddressCacheTime) {
        this.serviceAddressCacheTime = serviceAddressCacheTime;
    }

    public long getServiceCacheMaxCount() {
        return serviceCacheMaxCount;
    }

    public void setServiceCacheMaxCount(long serviceCacheMaxCount) {
        this.serviceCacheMaxCount = serviceCacheMaxCount;
    }
}
