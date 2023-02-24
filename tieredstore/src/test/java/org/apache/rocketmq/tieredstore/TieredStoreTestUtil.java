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
package org.apache.rocketmq.tieredstore;

import java.io.File;
import java.lang.reflect.Field;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.tieredstore.container.TieredContainerManager;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.Assert;

public class TieredStoreTestUtil {
    public static void destroyMetadataStore() {
        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(null);
        if (metadataStore != null) {
            metadataStore.destroy();
        }
        try {
            Field field = TieredStoreUtil.class.getDeclaredField("metadataStoreInstance");
            field.setAccessible(true);
            field.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
    }

    public static void destroyContainerManager() {
        TieredContainerManager containerManager = TieredContainerManager.getInstance(null);
        if (containerManager != null) {
            containerManager.destroy();
        }
        try {
            Field field = TieredContainerManager.class.getDeclaredField("instance");
            field.setAccessible(true);
            field.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
    }

    public static void destroyTempDir(String storePath) {
        try {
            FileUtils.deleteDirectory(new File(storePath));
        } catch (Exception ignore) {
        }
    }
}
