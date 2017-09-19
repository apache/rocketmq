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

package org.apache.rocketmq.rpc.internal;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Every error consists of error-latitude and error-longitude
 */
public class RpcErrorMapper {
    private RpcErrorLatitude errorLatitude;

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public enum RpcErrorLatitude {
        LOCAL("01"),
        REMOTE("02");

        private static Map<String, RpcErrorLatitude> stringToEnum = new HashMap<String, RpcErrorLatitude>();

        static {
            for (RpcErrorLatitude el : values()) {
                stringToEnum.put(el.getCode(), el);
            }
        }

        private String code;

        RpcErrorLatitude(String code) {
            this.code = code;
        }

        public static RpcErrorLatitude fromString(String cdoe) {
            return stringToEnum.get(cdoe);
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }
    }
}

