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
package org.apache.rocketmq.tieredstore.metrics;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.junit.Test;
import org.mockito.Mockito;

public class TieredStoreMetricsManagerTest {

    @Test
    public void getMetricsView() {
        TieredStoreMetricsManager.getMetricsView();
    }

    @Test
    public void init() {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        TieredMessageStore messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(Mockito.mock(FlatFileStore.class));
        MessageStoreFetcherImpl fetcher = Mockito.spy(new MessageStoreFetcherImpl(messageStore));

        TieredStoreMetricsManager.init(
            OpenTelemetrySdk.builder().build().getMeter(""),
            null, storeConfig, fetcher,
            Mockito.mock(FlatFileStore.class), Mockito.mock(DefaultMessageStore.class));
    }

    @Test
    public void newAttributesBuilder() {
        TieredStoreMetricsManager.newAttributesBuilder();
    }
}
