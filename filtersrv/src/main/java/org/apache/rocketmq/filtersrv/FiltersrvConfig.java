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

package org.apache.rocketmq.filtersrv;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class FiltersrvConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));

    private String connectWhichBroker = "127.0.0.1:10911";
    private String filterServerIP = RemotingUtil.getLocalAddress();

    private int compressMsgBodyOverHowmuch = 1024 * 8;
    private int zipCompressLevel = 5;

    private boolean clientUploadFilterClassEnable = true;

    private String filterClassRepertoryUrl = "http://fsrep.tbsite.net/filterclass";

    private int fsServerAsyncSemaphoreValue = 2048;
    private int fsServerCallbackExecutorThreads = 64;
    private int fsServerWorkerThreads = 64;

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getConnectWhichBroker() {
        return connectWhichBroker;
    }

    public void setConnectWhichBroker(String connectWhichBroker) {
        this.connectWhichBroker = connectWhichBroker;
    }

    public String getFilterServerIP() {
        return filterServerIP;
    }

    public void setFilterServerIP(String filterServerIP) {
        this.filterServerIP = filterServerIP;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public boolean isClientUploadFilterClassEnable() {
        return clientUploadFilterClassEnable;
    }

    public void setClientUploadFilterClassEnable(boolean clientUploadFilterClassEnable) {
        this.clientUploadFilterClassEnable = clientUploadFilterClassEnable;
    }

    public String getFilterClassRepertoryUrl() {
        return filterClassRepertoryUrl;
    }

    public void setFilterClassRepertoryUrl(String filterClassRepertoryUrl) {
        this.filterClassRepertoryUrl = filterClassRepertoryUrl;
    }

    public int getFsServerAsyncSemaphoreValue() {
        return fsServerAsyncSemaphoreValue;
    }

    public void setFsServerAsyncSemaphoreValue(int fsServerAsyncSemaphoreValue) {
        this.fsServerAsyncSemaphoreValue = fsServerAsyncSemaphoreValue;
    }

    public int getFsServerCallbackExecutorThreads() {
        return fsServerCallbackExecutorThreads;
    }

    public void setFsServerCallbackExecutorThreads(int fsServerCallbackExecutorThreads) {
        this.fsServerCallbackExecutorThreads = fsServerCallbackExecutorThreads;
    }

    public int getFsServerWorkerThreads() {
        return fsServerWorkerThreads;
    }

    public void setFsServerWorkerThreads(int fsServerWorkerThreads) {
        this.fsServerWorkerThreads = fsServerWorkerThreads;
    }
}
