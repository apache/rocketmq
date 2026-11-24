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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.reader.ObjectReader;
import com.alibaba.fastjson2.writer.ObjectWriter;

import java.lang.reflect.Type;
import java.util.Base64;
import java.util.BitSet;

public class BitSetSerializerDeserializer implements ObjectReader<BitSet>, ObjectWriter<BitSet> {

    @Override
    public void write(JSONWriter writer, Object object, Object fieldName, Type fieldType, long features) {
        if (object == null) {
            writer.writeBase64(null);
        } else {
            writer.writeBase64(((BitSet) object).toByteArray());
        }
    }

    @Override
    public BitSet readObject(JSONReader reader, Type fieldType, Object fieldName, long features) {
        if (reader.nextIfNull()) {
            return null;
        }
        String base64 = reader.readString();
        if (base64 == null || base64.isEmpty()) {
            return null;
        }
        byte[] bytes = Base64.getDecoder().decode(base64);
        return BitSet.valueOf(bytes);
    }

    @Override
    public long getFeatures() {
        return 0L;
    }

    @Override
    public Class<BitSet> getObjectClass() {
        return ObjectReader.super.getObjectClass();
    }
}
