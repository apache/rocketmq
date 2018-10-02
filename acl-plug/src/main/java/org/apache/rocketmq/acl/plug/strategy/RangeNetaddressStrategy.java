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
package org.apache.rocketmq.acl.plug.strategy;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plug.AclUtils;
import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class RangeNetaddressStrategy extends AbstractNetaddressStrategy {

    private String head;

    private int start;

    private int end;

    private int index;

    public RangeNetaddressStrategy(String netaddress) {
        String[] strArray = StringUtils.split(netaddress, ".");
        if (analysis(strArray, 2) || analysis(strArray, 3)) {
            verify(netaddress, index);
            StringBuffer sb = new StringBuffer().append(strArray[0].trim()).append(".").append(strArray[1].trim()).append(".");
            if (index == 3) {
                sb.append(strArray[2].trim()).append(".");
            }
            this.head = sb.toString();
        }
    }

    private boolean analysis(String[] strArray, int index) {
        String value = strArray[index].trim();
        this.index = index;
        if ("*".equals(value)) {
            setValue(0, 255);
        } else if (AclUtils.isMinus(value)) {
            String[] valueArray = StringUtils.split(value, "-");
            this.start = Integer.valueOf(valueArray[0]);
            this.end = Integer.valueOf(valueArray[1]);
            if (!(AclUtils.isScope(end) && AclUtils.isScope(start) && start <= end)) {

            }
        }
        return this.end > 0 ? true : false;
    }

    private void setValue(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean match(AccessControl accessControl) {
        String netAddress = accessControl.getNetaddress();
        if (netAddress.startsWith(this.head)) {
            String value;
            if (index == 3) {
                value = netAddress.substring(this.head.length());
            } else {
                value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.'));
            }
            Integer address = Integer.valueOf(value);
            if (address >= this.start && address <= this.end) {
                return true;
            }
        }
        return false;
    }

}
