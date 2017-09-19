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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class ThreadSafeKryo {
    private static final ThreadLocal<Kryo> KRYOS = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();

            kryo.register(byte[].class);
            kryo.register(char[].class);
            kryo.register(short[].class);
            kryo.register(int[].class);
            kryo.register(long[].class);
            kryo.register(float[].class);
            kryo.register(double[].class);
            kryo.register(boolean[].class);
            kryo.register(String[].class);
            kryo.register(Object[].class);
            kryo.register(KryoSerializable.class);
            kryo.register(BigInteger.class);
            kryo.register(BigDecimal.class);
            kryo.register(Class.class);
            kryo.register(Date.class);
            // kryo.register(Enum.class);
            kryo.register(EnumSet.class);
            kryo.register(Currency.class);
            kryo.register(StringBuffer.class);
            kryo.register(StringBuilder.class);
            kryo.register(Collections.EMPTY_LIST.getClass());
            kryo.register(Collections.EMPTY_MAP.getClass());
            kryo.register(Collections.EMPTY_SET.getClass());
            kryo.register(Collections.singletonList(null).getClass());
            kryo.register(Collections.singletonMap(null, null).getClass());
            kryo.register(Collections.singleton(null).getClass());
            kryo.register(TreeSet.class);
            kryo.register(Collection.class);
            kryo.register(TreeMap.class);
            kryo.register(Map.class);
            try {
                kryo.register(Class.forName("sun.util.calendar.ZoneInfo"));
            } catch (ClassNotFoundException e) {
                // Noop
            }
            kryo.register(Calendar.class);
            kryo.register(Locale.class);

            kryo.register(BitSet.class);
            kryo.register(HashMap.class);
            kryo.register(Timestamp.class);
            kryo.register(ArrayList.class);

            // kryo.setRegistrationRequired(true);
            kryo.setReferences(false);
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

            return kryo;
        }
    };

    public static Kryo getKryoInstance() {
        return KRYOS.get();
    }
}
