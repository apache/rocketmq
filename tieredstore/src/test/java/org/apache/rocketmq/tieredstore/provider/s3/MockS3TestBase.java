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

package org.apache.rocketmq.tieredstore.provider.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.junit.Assert;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MockS3TestBase {

    public static final String STORE_BASE_PATH = FileUtils.getTempDirectory() + File.separator + "MockS3TestBase-";

    protected S3MockStarterTestImpl s3MockStater;

    private String rootPath;

    protected void startMockedS3() {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(S3MockApplication.PROP_HTTP_PORT, S3MockApplication.RANDOM_PORT);
        properties.put(S3MockApplication.PROP_HTTPS_PORT, S3MockApplication.RANDOM_PORT);
        rootPath = STORE_BASE_PATH + UUID.randomUUID();
        properties.put(S3MockApplication.PROP_ROOT_DIRECTORY, rootPath);
        properties.put(S3MockApplication.PROP_INITIAL_BUCKETS, "rocketmq_lcy");

        TieredMessageStoreConfig config = new TieredMessageStoreConfig();
        config.setS3Region("ap-northeast-1");
        config.setS3Bucket("rocketmq-lcy");
        config.setS3AccessKey("");
        config.setS3SecretKey("");
        s3MockStater = new S3MockStarterTestImpl(properties);
        s3MockStater.start();
        TieredStorageS3Client client = MockS3AsyncClient.getMockTieredStorageS3Client(config, s3MockStater);
        try {
            Field instanceField = TieredStorageS3Client.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, client);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    protected void clearMockS3Data() {
        this.s3MockStater.stop();
        UtilAll.deleteFile(new File(rootPath));
    }
}
