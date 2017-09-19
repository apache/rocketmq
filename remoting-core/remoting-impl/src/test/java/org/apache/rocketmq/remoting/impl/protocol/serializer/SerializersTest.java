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

package org.apache.rocketmq.remoting.impl.protocol.serializer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.common.TypePresentation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.annotation.Message;

/**
 *
 */
public class SerializersTest {
    private static MsgPackSerializer msgPackSerializer;
    private static Kryo3Serializer kryo3Serializer;
    private static JsonSerializer jsonSerializer;

    @BeforeClass
    public static void init() {
        msgPackSerializer = new MsgPackSerializer();
        kryo3Serializer = new Kryo3Serializer();
        jsonSerializer = new JsonSerializer();
    }

    @Test
    public void msgPackSerializerTest() {
        runOneByOne(msgPackSerializer);
    }

    @Test
    public void kyroSerializerTest() {
        runOneByOne(kryo3Serializer);
    }

    @Test
    public void fastJsonSerializerTest() {
        runOneByOne(jsonSerializer);
    }

    private void runOneByOne(Serializer serializer) {
        listStringTest(serializer);
        listModelTest(serializer);
        modelTest(serializer);
        mapTest(serializer);
        listAndMapTest(serializer);
        arrayTest(serializer);
    }

    private void listStringTest(Serializer serializer) {
        // Create serialize objects.
        List<String> src = new ArrayList<>();
        src.add("msgpack");
        src.add("kumofs");
        src.add("viver");

        ByteBuffer srcBuf = serializer.encode(src);
        List<String> dst = serializer.decode(srcBuf.array(), new TypePresentation<List<String>>() {
        });
        Assert.assertEquals(dst, src);

        List<List<String>> ll = new ArrayList<>();
        ll.add(src);

        srcBuf = serializer.encode(ll);
        List<List<String>> llDst = serializer.decode(srcBuf.array(), new TypePresentation<List<List<String>>>() {
        });
        Assert.assertEquals(llDst, ll);

    }

    private void listModelTest(Serializer serializer) {
        // Create serialize objects.
        List<Model> src = new ArrayList<>();
        src.add(new Model(10));
        src.add(new Model(12));
        src.add(new Model(14));

        ByteBuffer srcBuf = serializer.encode(src);
        List<Model> dst = serializer.decode(srcBuf.array(), new TypePresentation<List<Model>>() {
        });
        Assert.assertEquals(dst, src);
    }

    private void modelTest(Serializer serializer) {
        Model src = new Model(123);

        ByteBuffer srcBuf = serializer.encode(src);

        Model dst = serializer.decode(srcBuf.array(), Model.class);
        Assert.assertEquals(dst, src);
    }

    private void mapTest(Serializer serializer) {
        Map<String, Model> src = new HashMap<>();

        src.put("foo", new Model(123));
        src.put("bar", new Model(234));

        ByteBuffer srcBuf = serializer.encode(src);

        Map<String, Model> dst = serializer.decode(srcBuf.array(), new TypePresentation<Map<String, Model>>() {
        });
        Assert.assertEquals(dst, src);
    }

    private void listAndMapTest(Serializer serializer) {
        Map<String, List<Model>> src = new HashMap<>();

        List<Model> list = new ArrayList<>();
        list.add(new Model(123));
        list.add(new Model(456));
        src.put("foo", list);

        ByteBuffer srcBuf = serializer.encode(src);

        Map<String, List<Model>> dst = serializer.decode(srcBuf.array(), new TypePresentation<Map<String, List<Model>>>() {
        });
        Assert.assertEquals(dst, src);
    }

    private void arrayTest(Serializer serializer) {
        Model[] models = new Model[3];
        models[0] = new Model(1);
        models[1] = new Model(2);
        models[2] = new Model(3);

        ByteBuffer srcBuf = serializer.encode(models);

        Model[] models1 = serializer.decode(srcBuf.array(), Model[].class);
        Assert.assertArrayEquals(models1, models);

        List<Model[]> arrayInList = new LinkedList<>();
        arrayInList.add(models);
        srcBuf = serializer.encode(arrayInList);
        List<Model[]> arrayInList1 = serializer.decode(srcBuf.array(), new TypePresentation<List<Model[]>>() {
        });
        Assert.assertArrayEquals(arrayInList.get(0), arrayInList1.get(0));
    }
}

@Message
class Model {
    private int a;

    private boolean isCCP = Boolean.TRUE;
    private byte multilingualer = Byte.MAX_VALUE;
    private short age = Short.MAX_VALUE;
    private char education = Character.MAX_VALUE;
    private int phoneNumber = Integer.MAX_VALUE;
    private long anniversary = Long.MAX_VALUE;
    private float cet4Score = Float.MAX_VALUE;
    private double cet6Score = Double.MAX_VALUE;

    private Map<String, Long> map = new HashMap<>();

    //protected Date birthday = Calendar.getInstance().getTime();
    private BigDecimal salary = BigDecimal.valueOf(11000.13);

    // protected TimeZone location = Calendar.getInstance().getTimeZone();
    //protected Timestamp location_time = new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis());

    //protected Locale locale = Locale.getDefault();
    //protected EnumSet certificates = EnumSet.allOf(Certificates.class);
    //protected BitSet qRCode = BitSet.valueOf(new long[]{123, 345});

    public Model() {
        init();
    }

    Model(final int a) {
        this.a = a;
        init();
    }

    private void init() {
        map.put("Hehe", 123L);
        map.put("grsgfg", 656L);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Model model = (Model) o;

        if (a != model.a)
            return false;
        if (isCCP != model.isCCP)
            return false;
        if (multilingualer != model.multilingualer)
            return false;
        if (age != model.age)
            return false;
        if (education != model.education)
            return false;
        if (phoneNumber != model.phoneNumber)
            return false;
        if (anniversary != model.anniversary)
            return false;
        if (Float.compare(model.cet4Score, cet4Score) != 0)
            return false;
        if (Double.compare(model.cet6Score, cet6Score) != 0)
            return false;
        if (map != null ? !map.equals(model.map) : model.map != null)
            return false;
        return salary != null ? salary.equals(model.salary) : model.salary == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = a;
        result = 31 * result + (isCCP ? 1 : 0);
        result = 31 * result + (int) multilingualer;
        result = 31 * result + (int) age;
        result = 31 * result + (int) education;
        result = 31 * result + phoneNumber;
        result = 31 * result + (int) (anniversary ^ (anniversary >>> 32));
        result = 31 * result + (cet4Score != +0.0f ? Float.floatToIntBits(cet4Score) : 0);
        temp = Double.doubleToLongBits(cet6Score);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (map != null ? map.hashCode() : 0);
        result = 31 * result + (salary != null ? salary.hashCode() : 0);
        return result;
    }

    public int getA() {
        return a;
    }

    public void setA(final int a) {
        this.a = a;
    }
}