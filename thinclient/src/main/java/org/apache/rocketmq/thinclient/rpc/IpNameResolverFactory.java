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

package org.apache.rocketmq.thinclient.rpc;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IpNameResolverFactory extends NameResolverProvider {
    private final List<EquivalentAddressGroup> addresses;
    private final String serviceAuthority = "IPAuthority";
    private NameResolver.Listener2 listener2;

    public IpNameResolverFactory(List<InetSocketAddress> socketAddresses) {
        this.addresses = convertAddresses(socketAddresses);
    }

    private List<EquivalentAddressGroup> convertAddresses(List<InetSocketAddress> socketAddresses) {
        ArrayList<EquivalentAddressGroup> addresses = new ArrayList<EquivalentAddressGroup>();
        for (InetSocketAddress socketAddress : socketAddresses) {
            addresses.add(new EquivalentAddressGroup(socketAddress));
        }
        Collections.shuffle(addresses);
        return addresses;
    }

    @Override
    public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {
        return new NameResolver() {

            @Override
            public String getServiceAuthority() {
                return serviceAuthority;
            }

            @Override
            public void start(NameResolver.Listener2 listener2) {
                IpNameResolverFactory.this.listener2 = listener2;
                listener2.onResult(ResolutionResult.newBuilder().setAddresses(addresses).build());
            }

            @Override
            public void shutdown() {
            }
        };
    }

    @Override
    public String getDefaultScheme() {
        return "IP";
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 0;
    }
}