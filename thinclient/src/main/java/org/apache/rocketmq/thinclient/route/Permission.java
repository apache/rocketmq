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

package org.apache.rocketmq.thinclient.route;

public enum Permission {
    /**
     * No any permission.
     */
    NONE,
    /**
     * Readable.
     */
    READ,
    /**
     * Writable.
     */
    WRITE,
    /**
     * Both of readable and writable.
     */
    READ_WRITE;

    public boolean isWritable() {
        switch (this) {
            case WRITE:
            case READ_WRITE:
                return true;
            case NONE:
            case READ:
            default:
                return false;
        }
    }

    public boolean isReadable() {
        switch (this) {
            case READ:
            case READ_WRITE:
                return true;
            case NONE:
            case WRITE:
            default:
                return false;
        }
    }

    public static Permission fromProtobuf(apache.rocketmq.v2.Permission permission) {
        switch (permission) {
            case READ:
                return Permission.READ;
            case WRITE:
                return Permission.WRITE;
            case READ_WRITE:
                return Permission.READ_WRITE;
            case NONE:
                return Permission.NONE;
            case PERMISSION_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Message queue permission is not specified");
        }
    }

    public static apache.rocketmq.v2.Permission toProtobuf(Permission permission) {
        switch (permission) {
            case READ:
                return apache.rocketmq.v2.Permission.READ;
            case WRITE:
                return apache.rocketmq.v2.Permission.WRITE;
            case READ_WRITE:
                return apache.rocketmq.v2.Permission.READ_WRITE;
            case NONE:
                return apache.rocketmq.v2.Permission.NONE;
            default:
                throw new IllegalArgumentException("Message queue permission is not specified");
        }
    }
}
