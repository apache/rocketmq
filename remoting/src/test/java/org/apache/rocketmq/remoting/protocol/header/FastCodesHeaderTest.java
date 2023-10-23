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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Test;

public class FastCodesHeaderTest {

    @Test
    public void testFastDecode() throws Exception {
        testFastDecode(SendMessageRequestHeaderV2.class);
        testFastDecode(SendMessageResponseHeader.class);
        testFastDecode(PullMessageRequestHeader.class);
        testFastDecode(PullMessageResponseHeader.class);
    }

    private void testFastDecode(Class<? extends CommandCustomHeader> classHeader) throws Exception {
        Field[] declaredFields = classHeader.getDeclaredFields();
        List<Field> declaredFieldsList = new ArrayList<>();
        for (Field f : declaredFields) {
            if (f.getName().startsWith("$")) {
                continue;
            }
            f.setAccessible(true);
            declaredFieldsList.add(f);
        }
        RemotingCommand command = RemotingCommand.createRequestCommand(0, null);
        HashMap<String, String> m = buildExtFields(declaredFieldsList);
        command.setExtFields(m);
        check(command, declaredFieldsList, classHeader);
    }

    private HashMap<String, String> buildExtFields(List<Field> fields) {
        HashMap<String, String> extFields = new HashMap<>();
        for (Field f: fields) {
            Class<?> c = f.getType();
            if (c.equals(String.class)) {
                extFields.put(f.getName(), "str");
            } else if (c.equals(Integer.class) || c.equals(int.class)) {
                extFields.put(f.getName(), "123");
            } else if (c.equals(Long.class) || c.equals(long.class)) {
                extFields.put(f.getName(), "1234");
            } else if (c.equals(Boolean.class) || c.equals(boolean.class)) {
                extFields.put(f.getName(), "true");
            } else {
                throw new RuntimeException(f.getName() + ":" + f.getType().getName());
            }
        }
        return extFields;
    }

    private void check(RemotingCommand command, List<Field> fields,
            Class<? extends CommandCustomHeader> classHeader) throws Exception {
        CommandCustomHeader o1 = command.decodeCommandCustomHeaderDirectly(classHeader, false);
        CommandCustomHeader o2 = classHeader.getDeclaredConstructor().newInstance();
        ((FastCodesHeader)o2).decode(command.getExtFields());
        for (Field f : fields) {
            Object value1 = f.get(o1);
            Object value2 = f.get(o2);
            if (value1 == null) {
                Assert.assertNull(value2);
            } else {
                Assert.assertEquals(value1, value2);
            }
        }
    }

}
