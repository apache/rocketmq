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
package org.apache.rocketmq.common.action;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ActionTest {

    @Test
    public void getByName_NullName_ReturnsNull() {
        Action result = Action.getByName("null");
        assertNull(result);
    }

    @Test
    public void getByName_UnknownName_ReturnsUnknown() {
        Action result = Action.getByName("unknown");
        assertEquals(Action.UNKNOWN, result);
    }

    @Test
    public void getByName_AllName_ReturnsAll() {
        Action result = Action.getByName("All");
        assertEquals(Action.ALL, result);
    }

    @Test
    public void getByName_AnyName_ReturnsAny() {
        Action result = Action.getByName("Any");
        assertEquals(Action.ANY, result);
    }

    @Test
    public void getByName_PubName_ReturnsPub() {
        Action result = Action.getByName("Pub");
        assertEquals(Action.PUB, result);
    }

    @Test
    public void getByName_SubName_ReturnsSub() {
        Action result = Action.getByName("Sub");
        assertEquals(Action.SUB, result);
    }

    @Test
    public void getByName_CreateName_ReturnsCreate() {
        Action result = Action.getByName("Create");
        assertEquals(Action.CREATE, result);
    }

    @Test
    public void getByName_UpdateName_ReturnsUpdate() {
        Action result = Action.getByName("Update");
        assertEquals(Action.UPDATE, result);
    }

    @Test
    public void getByName_DeleteName_ReturnsDelete() {
        Action result = Action.getByName("Delete");
        assertEquals(Action.DELETE, result);
    }

    @Test
    public void getByName_GetName_ReturnsGet() {
        Action result = Action.getByName("Get");
        assertEquals(Action.GET, result);
    }

    @Test
    public void getByName_ListName_ReturnsList() {
        Action result = Action.getByName("List");
        assertEquals(Action.LIST, result);
    }

    @Test
    public void getCode_ReturnsCorrectCode() {
        assertEquals((byte) 1, Action.ALL.getCode());
        assertEquals((byte) 2, Action.ANY.getCode());
        assertEquals((byte) 3, Action.PUB.getCode());
        assertEquals((byte) 4, Action.SUB.getCode());
        assertEquals((byte) 5, Action.CREATE.getCode());
        assertEquals((byte) 6, Action.UPDATE.getCode());
        assertEquals((byte) 7, Action.DELETE.getCode());
        assertEquals((byte) 8, Action.GET.getCode());
        assertEquals((byte) 9, Action.LIST.getCode());
    }

    @Test
    public void getName_ReturnsCorrectName() {
        assertEquals("All", Action.ALL.getName());
        assertEquals("Any", Action.ANY.getName());
        assertEquals("Pub", Action.PUB.getName());
        assertEquals("Sub", Action.SUB.getName());
        assertEquals("Create", Action.CREATE.getName());
        assertEquals("Update", Action.UPDATE.getName());
        assertEquals("Delete", Action.DELETE.getName());
        assertEquals("Get", Action.GET.getName());
        assertEquals("List", Action.LIST.getName());
    }
}
