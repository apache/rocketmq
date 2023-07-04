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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class RequestCodeTest {

    @Test
    public void testRequestCodeShouldBeShort() throws IllegalAccessException {
        Set<String> ignored = new HashSet<>();
        ignored.add("POP_MESSAGE");
        ignored.add("ACK_MESSAGE");
        ignored.add("BATCH_ACK_MESSAGE");
        ignored.add("PEEK_MESSAGE");
        ignored.add("CHANGE_MESSAGE_INVISIBLETIME");
        ignored.add("NOTIFICATION");
        ignored.add("POLLING_INFO");

        Map<Integer, String> occupied = new HashMap<>();
        Class clazz = RequestCode.class;
        Field[] fields = clazz.getFields();
        for (Field field : fields) {
            if (ignored.contains(field.getName())) {
                continue;
            }
            if (field.getInt(clazz) > Short.MAX_VALUE) {
                Assert.fail(field.getName() + "=" + field.getInt(clazz) + " should be short, to support serializeTypeCurrentRPC=ROCKETMQ");
            }
            String name = occupied.get(field.getInt(clazz));
            if (name != null) {
                Assert.fail(field.getName() + "=" + field.getInt(clazz) + " is occupied by " + name);
            }
            occupied.put(field.getInt(clazz), field.getName());
        }
    }
}
