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

package org.apache.rocketmq.thinclient.misc;

import java.io.InputStream;
import java.util.Properties;

public class MetadataUtils {
    private static final String METADATA_CONF_PATH = "rocketmq.metadata.properties";
    private static final String WRAPPER_METADATA_CONF_PATH = "rocketmq.wrapper.metadata.properties";

    private static final Properties METADATA_PROPERTIES = new Properties();
    private static final Properties WRAPPER_METADATA_PROPERTIES = new Properties();

    private static final String VERSION_KEY = "rocketmq.version";
    private static final String WRAPPER_VERSION_KEY = "rocketmq.wrapper.version";

    private MetadataUtils() {
    }

    private static void load(String path, Properties properties) {
        try (InputStream stream = MetadataUtils.class.getClassLoader().getResourceAsStream(path)) {
            properties.load(stream);
        } catch (Throwable ignore) {
            // ignore on purpose.
        }
    }

    static {
        load(METADATA_CONF_PATH, METADATA_PROPERTIES);
        load(WRAPPER_METADATA_CONF_PATH, WRAPPER_METADATA_PROPERTIES);
    }

    public static String getVersion() {
        return METADATA_PROPERTIES.getProperty(VERSION_KEY);
    }

    public static String getWrapperVersion() {
        return WRAPPER_METADATA_PROPERTIES.getProperty(WRAPPER_VERSION_KEY);
    }
}
