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

package org.apache.rocketmq.remoting.internal;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PropertyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyUtils.class);

    public static String getPropertyIgnoreCase(final Properties properties, final String key) {
        String value = null;
        if (properties != null) {
            for (Map.Entry<Object, Object> next : properties.entrySet()) {
                if (next.getKey().toString().equalsIgnoreCase(key)) {
                    return next.getValue().toString();
                }
            }
        }
        return value;
    }

    /**
     * Read a properties file from the given path
     *
     * @param filename The path of the file to read
     * @return Property file instance
     */
    public static Properties loadProps(String filename) {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
            props.load(propStream);
        } catch (IOException e) {
            LOG.error(String.format("Loading properties from file %s error !", filename), e);
            System.exit(1);
        }
        return props;
    }

}
