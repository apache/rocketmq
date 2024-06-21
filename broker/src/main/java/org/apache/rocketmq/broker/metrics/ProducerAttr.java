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
package org.apache.rocketmq.broker.metrics;

import com.google.common.base.Objects;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class ProducerAttr {
    LanguageCode language;
    int version;

    public ProducerAttr(LanguageCode language, int version) {
        this.language = language;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProducerAttr attr = (ProducerAttr) o;
        return version == attr.version && language == attr.language;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(language, version);
    }
}
