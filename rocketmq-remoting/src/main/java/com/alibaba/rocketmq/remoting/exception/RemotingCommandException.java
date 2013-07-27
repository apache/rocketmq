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
package com.alibaba.rocketmq.remoting.exception;

/**
 * 命令解析自定义字段时，校验字段有效性抛出异常
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class RemotingCommandException extends RemotingException {
    private static final long serialVersionUID = -6061365915274953096L;


    public RemotingCommandException(String message) {
        super(message, null);
    }


    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
