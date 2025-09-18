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
package org.apache.rocketmq.remoting.protocol.header;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.MixAll;
import org.junit.Assert;
import org.junit.Test;

public class ExportRocksDBConfigToJsonRequestHeaderTest {
    @Test
    public void configTypeTest() {
        if (MixAll.isMac()) {
            return;
        }
        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> configTypes = new ArrayList<>();
        configTypes.add(ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS);
        configTypes.add(ExportRocksDBConfigToJsonRequestHeader.ConfigType.SUBSCRIPTION_GROUPS);

        String string = ExportRocksDBConfigToJsonRequestHeader.ConfigType.toString(configTypes);

        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> newConfigTypes = ExportRocksDBConfigToJsonRequestHeader.ConfigType.fromString(string);
        assert newConfigTypes.size() == 2;
        assert configTypes.equals(newConfigTypes);

        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> topics = ExportRocksDBConfigToJsonRequestHeader.ConfigType.fromString("topics");
        assert topics.size() == 1;
        assert topics.get(0).equals(ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS);

        List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> mix = ExportRocksDBConfigToJsonRequestHeader.ConfigType.fromString("toPics; subScriptiongroups");
        assert mix.size() == 2;
        assert mix.get(0).equals(ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS);
        assert mix.get(1).equals(ExportRocksDBConfigToJsonRequestHeader.ConfigType.SUBSCRIPTION_GROUPS);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            ExportRocksDBConfigToJsonRequestHeader.ConfigType.fromString("topics; subscription");
        });

    }
}
