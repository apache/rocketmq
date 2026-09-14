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

package org.apache.rocketmq.remoting.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum LanguageCode {
    JAVA((byte) 0),
    CPP((byte) 1),
    DOTNET((byte) 2),
    PYTHON((byte) 3),
    DELPHI((byte) 4),
    ERLANG((byte) 5),
    RUBY((byte) 6),
    OTHER((byte) 7),
    HTTP((byte) 8),
    GO((byte) 9),
    PHP((byte) 10),
    OMS((byte) 11),
    RUST((byte) 12),
    NODE_JS((byte) 13);

    private static final LanguageCode[] BY_CODE;
    static {
        LanguageCode[] all = values();
        int max = 0;
        for (LanguageCode lc : all) {
            max = Math.max(max, lc.code & 0xFF);
        }
        BY_CODE = new LanguageCode[max + 1];
        for (LanguageCode lc : all) {
            BY_CODE[lc.code & 0xFF] = lc;
        }
    }

    private byte code;

    LanguageCode(byte code) {
        this.code = code;
    }

    public static LanguageCode valueOf(byte code) {
        int idx = code & 0xFF;
        return idx < BY_CODE.length ? BY_CODE[idx] : null;
    }

    public byte getCode() {
        return code;
    }
    
    private static final Map<String, LanguageCode> MAP = Arrays.stream(LanguageCode.values()).collect(Collectors.toMap(LanguageCode::name, Function.identity()));

    public static LanguageCode getCode(String language) {
        return MAP.get(language);
    }
}
