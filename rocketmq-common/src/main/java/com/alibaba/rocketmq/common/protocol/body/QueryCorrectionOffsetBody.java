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
package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-08-06
 */
public class QueryCorrectionOffsetBody extends RemotingSerializable {
    private Map<Integer, Long> correctionOffsets = new HashMap<Integer, Long>();


    public Map<Integer, Long> getCorrectionOffsets() {
        return correctionOffsets;
    }


    public void setCorrectionOffsets(Map<Integer, Long> correctionOffsets) {
        this.correctionOffsets = correctionOffsets;
    }
}
