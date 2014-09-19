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
package com.alibaba.rocketmq.common;

/**
 * 服务对象的状态，通常需要start，shutdown
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum ServiceState {
    /**
     * 服务对象刚刚创建，但是未启动
     */
    CREATE_JUST,
    /**
     * 服务启动成功
     */
    RUNNING,
    /**
     * 服务已经关闭
     */
    SHUTDOWN_ALREADY,
    /**
     * 服务启动失败
     */
    START_FAILED
}
