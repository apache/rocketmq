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

package org.apache.rocketmq.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter factory: support other filter to register.
 */
public class FilterFactory {

    public static final FilterFactory INSTANCE = new FilterFactory();

    protected static final Map<String, FilterSpi> FILTER_SPI_HOLDER = new HashMap<>(4);

    static {
        FilterFactory.INSTANCE.register(new SqlFilter());
    }

    /**
     * Register a filter.
     * <br>
     * Note:
     * <li>1. Filter registered will be used in broker server, so take care of it's reliability and performance.</li>
     */
    public void register(FilterSpi filterSpi) {
        if (FILTER_SPI_HOLDER.containsKey(filterSpi.ofType())) {
            throw new IllegalArgumentException(String.format("Filter spi type(%s) already exist!", filterSpi.ofType()));
        }

        FILTER_SPI_HOLDER.put(filterSpi.ofType(), filterSpi);
    }

    /**
     * Un register a filter.
     */
    public FilterSpi unRegister(String type) {
        return FILTER_SPI_HOLDER.remove(type);
    }

    /**
     * Get a filter registered, null if none exist.
     */
    public FilterSpi get(String type) {
        return FILTER_SPI_HOLDER.get(type);
    }

}
