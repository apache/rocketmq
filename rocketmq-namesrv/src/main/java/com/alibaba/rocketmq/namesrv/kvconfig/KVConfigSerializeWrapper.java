/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.namesrv.kvconfig;

import java.util.HashMap;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * KV配置序列化，json包装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class KVConfigSerializeWrapper extends RemotingSerializable {
    private HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable;


    public HashMap<String, HashMap<String, String>> getConfigTable() {
        return configTable;
    }


    public void setConfigTable(HashMap<String, HashMap<String, String>> configTable) {
        this.configTable = configTable;
    }
}
