/**
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

package com.alibaba.rocketmq.namesrv.kvconfig;

import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by xigu.lx on 2016/11/14.
 *
 * manager of name server's config
 */
public class NamesrvConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private DataVersion dataVersion = new DataVersion();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private NamesrvController namesrvController;

    public NamesrvConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    private String getStorePath() {
        return this.namesrvController.getNamesrvConfig().getConfigStorePath();
    }

    /**
     * read config from file when start
     */
    public void load() {
        String path = getStorePath();
        String configStr = MixAll.file2String(path);
        if (configStr == null) {
            log.info("load name server config encounter null file!");
            configStr = MixAll.file2String(path + ".bak");
            if (configStr == null) {
                log.info("load name server config encounter null bak file!");
                return;
            }
        }

        Properties properties = MixAll.string2Properties(configStr);

        MixAll.properties2Object(properties, this.namesrvController.getNamesrvConfig());
        MixAll.properties2Object(properties, this.namesrvController.getNettyServerConfig());
        MixAll.properties2Object(properties, this.dataVersion);

        log.info("load name server config ok!\n{}", getConfigs());
    }

    /**
     * write the config in memory to file
     */
    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();

            try {
                String configs = getConfigs();
                if (configs == null) {
                    return;
                }

                MixAll.string2File(configs, getStorePath());

                log.info("persist name server config ok!\n{}", configs);
            } catch (IOException e) {
                log.error("persist name server config error, ", e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist name server config error, ", e);
        }
    }

    /**
     * will persist after update
     *
     * @param properties
     */
    public void update(Properties properties) {
        try {
            this.lock.writeLock().lockInterruptibly();

            try {
                MixAll.properties2Object(properties, this.namesrvController.getNamesrvConfig());
                MixAll.properties2Object(properties, this.namesrvController.getNettyServerConfig());
                this.dataVersion.nextVersion();
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update name server config error, ", e);
        }

        this.persist();
    }

    /**
     * json string of data version
     *
     * @return
     */
    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    /**
     * build the config in memory to string, include
     *
     * <li>1. NameSrvConfig</li>
     * <li>2. Netty Server Config</li>
     * <li>3. Data Version</li>
     *
     * @return
     */
    public String getConfigs() {
        String config = null;
        try {
            this.lock.readLock().lockInterruptibly();

            try {
                config = getConfigsInternal();
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("get name server config error, ", e);
        }

        return config;
    }

    private String getConfigsInternal() {
        StringBuilder sb = new StringBuilder();

        {
            Properties properties = MixAll.object2Properties(this.namesrvController.getNamesrvConfig());
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.warn("Name server config is null!");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.namesrvController.getNettyServerConfig());
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.warn("Netty server config of name server is null!");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.dataVersion);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.warn("Data version of name server is null!");
            }
        }

        return sb.toString();
    }
}

