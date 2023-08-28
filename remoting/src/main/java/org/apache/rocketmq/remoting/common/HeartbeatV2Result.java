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
package org.apache.rocketmq.remoting.common;

public class HeartbeatV2Result {
    private int version = 0;
    private boolean isSubChange = false;
    private boolean isSupportV2 = false;

    public HeartbeatV2Result(int version, boolean isSubChange, boolean isSupportV2) {
        this.version = version;
        this.isSubChange = isSubChange;
        this.isSupportV2 = isSupportV2;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isSubChange() {
        return isSubChange;
    }

    public void setSubChange(boolean subChange) {
        isSubChange = subChange;
    }

    public boolean isSupportV2() {
        return isSupportV2;
    }

    public void setSupportV2(boolean supportV2) {
        isSupportV2 = supportV2;
    }
}
