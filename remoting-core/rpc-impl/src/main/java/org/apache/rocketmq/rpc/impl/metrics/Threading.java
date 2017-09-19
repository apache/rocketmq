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

package org.apache.rocketmq.rpc.impl.metrics;

public class Threading implements Comparable {
    private String name;
    private long id;

    public Threading() {
    }

    public Threading(String name, long id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Threading threading = (Threading) o;

        return id == threading.id && !(name != null ? !name.equals(threading.name) : threading.name != null);

    }

    @Override
    public String toString() {
        return "Threading{" +
            "name='" + name + '\'' +
            ", id=" + id +
            '}';
    }

    @Override
    public int compareTo(Object o) {
        Threading t = (Threading) o;
        int ret = t.name.compareTo(this.name);
        if (ret == 0) {
            return Long.valueOf(t.id).compareTo(this.id);
        }

        return ret;
    }

}
