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

package org.apache.rocketmq.common.filter;

public class ExpressionType {

    /**
     * <ul>
     * Keywords:
     * <li>{@code AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL}</li>
     * </ul>
     * <p/>
     * <ul>
     * Data type:
     * <li>Boolean, like: TRUE, FALSE</li>
     * <li>String, like: 'abc'</li>
     * <li>Decimal, like: 123</li>
     * <li>Float number, like: 3.1415</li>
     * </ul>
     * <p/>
     * <ul>
     * Grammar:
     * <li>{@code AND, OR}</li>
     * <li>{@code >, >=, <, <=, =}</li>
     * <li>{@code BETWEEN A AND B}, equals to {@code >=A AND <=B}</li>
     * <li>{@code NOT BETWEEN A AND B}, equals to {@code >B OR <A}</li>
     * <li>{@code IN ('a', 'b')}, equals to {@code ='a' OR ='b'}, this operation only support String type.</li>
     * <li>{@code IS NULL}, {@code IS NOT NULL}, check parameter whether is null, or not.</li>
     * <li>{@code =TRUE}, {@code =FALSE}, check parameter whether is true, or false.</li>
     * </ul>
     * <p/>
     * <p>
     * Example:
     * (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
     * </p>
     */
    public static final String SQL92 = "SQL92";

    /**
     * Only support or operation such as
     * "tag1 || tag2 || tag3", <br>
     * If null or * expression,meaning subscribe all.
     */
    public static final String TAG = "TAG";

    public static boolean isTagType(String type) {
        if (type == null || "".equals(type) || TAG.equals(type)) {
            return true;
        }
        return false;
    }
}
