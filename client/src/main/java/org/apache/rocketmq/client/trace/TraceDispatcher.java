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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.exception.MQClientException;
import java.io.IOException;

/**
 * Interface of asynchronous transfer data
 * 异步传输数据接口 （消息跟踪）
 */
public interface TraceDispatcher {

    /**
     * Initialize asynchronous transfer data module
     * 初始化异步传输数据模块
     */
    void start(String nameSrvAddr) throws MQClientException;

    /**
     * Append the transfering data
     * 追加传输数据
     * @param ctx data infomation
     * @return
     */
    boolean append(Object ctx);

    /**
     * Write flush action
     * 写入刷新
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close the trace Hook
     * 关闭跟踪钩子
     */
    void shutdown();
}
