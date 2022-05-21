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

package org.apache.rocketmq.thinclient.message.protocol;

public enum Encoding {
    IDENTITY,
    GZIP;

    public static apache.rocketmq.v2.Encoding toProtobuf(Encoding encoding) {
        switch (encoding) {
            case IDENTITY:
                return apache.rocketmq.v2.Encoding.IDENTITY;
            case GZIP:
                return apache.rocketmq.v2.Encoding.GZIP;
            default:
                return apache.rocketmq.v2.Encoding.ENCODING_UNSPECIFIED;
        }
    }

    public static Encoding fromProtobuf(apache.rocketmq.v2.Encoding encoding) {
        switch (encoding) {
            case IDENTITY:
                return IDENTITY;
            case GZIP:
                return GZIP;
            case ENCODING_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Encoding is not specified");
        }
    }
}
