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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 配置管理
 */
public abstract class ConfigManager {
    private static final Logger PLOG = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 编码内容
     * @return 编码后的内容
     */
    public abstract String encode();

    /**
     * 加载文件
     *
     * @return 加载是否成功
     */
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            // 如果内容不存在，则加载备份文件
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                PLOG.info("load {} OK", fileName);
                return true;
            }
        } catch (Exception e) {
            PLOG.error("load " + fileName + " Failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    /**
     * 配置文件地址
     *
     * @return 配置文件地址
     */
    public abstract String configFilePath();

    /**
     * 加载备份文件
     *
     * @return 是否成功
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                PLOG.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            PLOG.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    /**
     * 解码内容
     *
     * @param jsonString 内容
     */
    public abstract void decode(final String jsonString);

    /**
     * 持久化
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                PLOG.error("persist file Exception, " + fileName, e);
            }
        }
    }

    /**
     * 编码存储内容
     *
     * @param prettyFormat 是否格式化
     * @return 内容
     */
    public abstract String encode(final boolean prettyFormat);
}
