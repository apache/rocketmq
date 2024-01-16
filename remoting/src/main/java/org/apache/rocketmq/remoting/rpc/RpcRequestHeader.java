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
package org.apache.rocketmq.remoting.rpc;

import com.google.common.base.MoreObjects;
import java.util.Objects;
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

    @Deprecated
    public String getBname() {
        return bname;
    }

    @Deprecated
    public void setBname(String brokerName) {
        this.bname = brokerName;
    }

    public String getBrokerName() {
        return bname;
    }

    public void setBrokerName(String brokerName) {
        this.bname = brokerName;
    }

    public String getNamespace() {
        return ns;
    }

    public void setNamespace(String namespace) {
        this.ns = namespace;
    }

    public Boolean getNamespaced() {
        return nsd;
    }

    public void setNamespaced(Boolean namespaced) {
        this.nsd = namespaced;
    }

    public Boolean getOneway() {
        return oway;
    }

    public void setOneway(Boolean oneway) {
        this.oway = oneway;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RpcRequestHeader header = (RpcRequestHeader) o;
        return Objects.equals(ns, header.ns) && Objects.equals(nsd, header.nsd) && Objects.equals(bname, header.bname) && Objects.equals(oway, header.oway);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ns, nsd, bname, oway);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("namespace", ns)
            .add("namespaced", nsd)
            .add("brokerName", bname)
            .add("oneway", oway)
            .toString();
    }
}
