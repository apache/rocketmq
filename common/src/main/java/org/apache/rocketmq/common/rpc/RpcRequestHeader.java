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
package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.remoting.CommandCustomHeader;

public abstract class RpcRequestHeader implements CommandCustomHeader {
    //the namespace name
    protected String ns;
    //if the data has been namespaced
    protected Boolean nsd;
    //the abstract remote addr name, usually the physical broker name
    protected String bname;
    //oneway
    protected Boolean oway;

    public String getBname() {
        return bname;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }

    public Boolean getOway() {
        return oway;
    }

    public void setOway(Boolean oway) {
        this.oway = oway;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public Boolean getNsd() {
        return nsd;
    }

    public void setNsd(Boolean nsd) {
        this.nsd = nsd;
    }
}
