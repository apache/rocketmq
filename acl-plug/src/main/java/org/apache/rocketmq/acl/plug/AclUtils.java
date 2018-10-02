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
package org.apache.rocketmq.acl.plug;

import org.apache.commons.lang3.StringUtils;

public class AclUtils {

    public static String[] getAddreeStrArray(String netaddress, String four) {
        String[] fourStrArray = StringUtils.split(four.substring(1, four.length() - 1), ",");
        String address = netaddress.substring(0, netaddress.indexOf("{"));
        String[] addreeStrArray = new String[fourStrArray.length];
        for (int i = 0; i < fourStrArray.length; i++) {
            addreeStrArray[i] = address + fourStrArray[i];
        }
        return addreeStrArray;
    }

    public static boolean isScope(String num, int index) {
        String[] strArray = StringUtils.split(num, ".");
        if (strArray.length != 4) {
            return false;
        }
        return isScope(strArray, index);

    }

    public static boolean isScope(String[] num, int index) {
        if (num.length <= index) {

        }
        for (int i = 0; i < index; i++) {
            if (!isScope(num[i])) {
                return false;
            }
        }
        return true;

    }

    public static boolean isScope(String num) {
        return isScope(Integer.valueOf(num.trim()));
    }

    public static boolean isScope(int num) {
        return num >= 0 && num <= 255;
    }

    public static boolean isAsterisk(String asterisk) {
        return asterisk.indexOf('*') > -1;
    }

    public static boolean isColon(String colon) {
        return colon.indexOf(',') > -1;
    }

    public static boolean isMinus(String minus) {
        return minus.indexOf('-') > -1;

    }
}
