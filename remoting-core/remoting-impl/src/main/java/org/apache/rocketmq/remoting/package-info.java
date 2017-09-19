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

/**
 * This package contains all the transport classes that can be reused any times.
 *
 * Remoting wire-format protocol description:
 *
 * <pre>
 * 2015-04-29 16:07:14 v1.0
 * 2016-04-23 16:18:05 v2.0
 * 2016-05-31 09:33:11 v3.0
 * 2016-11-10 09:33:11 v3.1 remove deprecated tag field
 *
 *
 * 1.Protocol Type                            1 byte
 * 2.Total Length                             4 byte,exclude protocol type size
 * 3.RequestID                                4 byte,used for repeatable requests,connection reuse.an requestID string
 * representing a client-generated, globally unique for some time unit, identifier for the request
 * 4.Serializer Type                          1 byte
 * 5.Traffic Type                             1 byte,0-sync;1-async;2-oneway;3-response
 * 6.OpCode Length                            2 byte
 * 7.OpCode                                   variant length,utf8 string
 * 8.Remark Length                            2 byte
 * 9.Remark                                   variant length,utf8 string
 * 10.Properties Size                         2 byte
 * Property Length                            2 byte
 * Property Body                              variant length,utf8,Key\nValue
 * 11.Inbound or OutBound payload length      4 byte
 * 12.Inbound or OutBound payload             variant length, max size limitation is 16M
 * 13.Extra payload                           variant length
 *
 * </pre>
 */
package org.apache.rocketmq.remoting;