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
package org.apache.rocketmq.common.namesrv;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Map;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.utils.HttpTinyClient;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class DefaultTopAddressing implements TopAddressing {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String nsAddr;
    private String wsAddr;
    private String unitName;
    private Map<String, String> para;
    private List<TopAddressing> topAddressingList;

    public DefaultTopAddressing(final String wsAddr) {
        this(wsAddr, null);
    }

    public DefaultTopAddressing(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.topAddressingList = loadCustomTopAddressing();
    }

    public DefaultTopAddressing(final String unitName, final Map<String, String> para, final String wsAddr) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.para = para;
        this.topAddressingList = loadCustomTopAddressing();
    }

    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    private List<TopAddressing> loadCustomTopAddressing() {
        ServiceLoader<TopAddressing> serviceLoader = ServiceLoader.load(TopAddressing.class);
        Iterator<TopAddressing> iterator = serviceLoader.iterator();
        List<TopAddressing> topAddressingList = new ArrayList<>();
        if (iterator.hasNext()) {
            topAddressingList.add(iterator.next());
        }
        return topAddressingList;
    }

    @Override
    public final String fetchNSAddr() {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                String nsAddress = topAddressing.fetchNSAddr();
                if (!Strings.isNullOrEmpty(nsAddress)) {
                    return nsAddress;
                }
            }
        }
        // Return result of default implementation
        return fetchNSAddr(true, 3000);
    }

    @Override
    public void registerChangeCallBack(NameServerUpdateCallback changeCallBack) {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                topAddressing.registerChangeCallBack(changeCallBack);
            }
        }
    }

    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        String url = this.wsAddr;
        try {
            if (null != para && para.size() > 0) {
                if (!UtilAll.isBlank(this.unitName)) {
                    url = url + "-" + this.unitName + "?nofix=1&";
                }
                else {
                    url = url + "?";
                }
                for (Map.Entry<String, String> entry : this.para.entrySet()) {
                    url += entry.getKey() + "=" + entry.getValue() + "&";
                }
                url = url.substring(0, url.length() - 1);
            }
            else {
                if (!UtilAll.isBlank(this.unitName)) {
                    url = url + "-" + this.unitName + "?nofix=1";
                }
            }

            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {
                    LOGGER.error("fetch nameserver address is null");
                }
            } else {
                LOGGER.error("fetch nameserver address failed. statusCode=" + result.code);
            }
        } catch (IOException e) {
            if (verbose) {
                LOGGER.error("fetch name server address exception", e);
            }
        }

        if (verbose) {
            String errorMsg =
                "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            LOGGER.warn(errorMsg);
        }
        return null;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
