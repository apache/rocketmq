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
package org.apache.rocketmq.store.metrics;

import io.opentelemetry.api.common.AttributesBuilder;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.store.MessageStore;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;

/**
 * Store metrics manager interface for different message store implementations.
 * This interface provides a unified way to access metrics functionality
 * regardless of the underlying message store type.
 */
public interface StoreMetricsManager {

    /**
     * Initialize metrics with the given meter and attributes builder supplier.
     *
     * @param meter                     OpenTelemetry meter
     * @param attributesBuilderSupplier Metrics attributes builder supplier
     * @param messageStore             The message store instance
     */
    void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier, MessageStore messageStore);

    /**
     * Get metrics view configuration.
     *
     * @return List of instrument selector and view builder pairs
     */
    List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView();

}
