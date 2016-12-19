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
package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;


/**
 * @author shijia.wxr
 *
 */
public class KVTable extends RemotingSerializable {
    private HashMap<String, String> table = new HashMap<String, String>();


    public HashMap<String, String> getTable() {
        return table;
    }


    public void setTable(HashMap<String, String> table) {
        this.table = table;
    }
}
