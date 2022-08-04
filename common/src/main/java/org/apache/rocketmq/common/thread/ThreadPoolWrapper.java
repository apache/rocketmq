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

package org.apache.rocketmq.common.thread;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolWrapper {
    private String name;
    private ThreadPoolExecutor threadPoolExecutor;
    private List<ThreadPoolStatusMonitor> statusPrinters;

    ThreadPoolWrapper(final String name, final ThreadPoolExecutor threadPoolExecutor,
        final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.name = name;
        this.threadPoolExecutor = threadPoolExecutor;
        this.statusPrinters = statusPrinters;
    }

    public static class ThreadPoolWrapperBuilder {
        private String name;
        private ThreadPoolExecutor threadPoolExecutor;
        private List<ThreadPoolStatusMonitor> statusPrinters;

        ThreadPoolWrapperBuilder() {
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder name(final String name) {
            this.name = name;
            return this;
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder threadPoolExecutor(
            final ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
            return this;
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder statusPrinters(
            final List<ThreadPoolStatusMonitor> statusPrinters) {
            this.statusPrinters = statusPrinters;
            return this;
        }

        public ThreadPoolWrapper build() {
            return new ThreadPoolWrapper(this.name, this.threadPoolExecutor, this.statusPrinters);
        }

        @java.lang.Override
        public java.lang.String toString() {
            return "ThreadPoolWrapper.ThreadPoolWrapperBuilder(name=" + this.name + ", threadPoolExecutor=" + this.threadPoolExecutor + ", statusPrinters=" + this.statusPrinters + ")";
        }
    }

    public static ThreadPoolWrapper.ThreadPoolWrapperBuilder builder() {
        return new ThreadPoolWrapper.ThreadPoolWrapperBuilder();
    }

    public String getName() {
        return this.name;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    public List<ThreadPoolStatusMonitor> getStatusPrinters() {
        return this.statusPrinters;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setThreadPoolExecutor(final ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public void setStatusPrinters(final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.statusPrinters = statusPrinters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThreadPoolWrapper wrapper = (ThreadPoolWrapper) o;
        return Objects.equal(name, wrapper.name) && Objects.equal(threadPoolExecutor, wrapper.threadPoolExecutor) && Objects.equal(statusPrinters, wrapper.statusPrinters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, threadPoolExecutor, statusPrinters);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("threadPoolExecutor", threadPoolExecutor)
            .add("statusPrinters", statusPrinters)
            .toString();
    }
}
