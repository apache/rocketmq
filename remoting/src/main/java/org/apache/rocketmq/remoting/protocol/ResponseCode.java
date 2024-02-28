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

package org.apache.rocketmq.remoting.protocol;

public class ResponseCode extends RemotingSysResponseCode {

    public static final int FLUSH_DISK_TIMEOUT = 10;

    public static final int SLAVE_NOT_AVAILABLE = 11;

    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    public static final int MESSAGE_ILLEGAL = 13;

    public static final int SERVICE_NOT_AVAILABLE = 14;

    public static final int VERSION_NOT_SUPPORTED = 15;

    public static final int NO_PERMISSION = 16;

    public static final int TOPIC_NOT_EXIST = 17;
    public static final int TOPIC_EXIST_ALREADY = 18;
    public static final int PULL_NOT_FOUND = 19;

    public static final int PULL_RETRY_IMMEDIATELY = 20;

    public static final int PULL_OFFSET_MOVED = 21;

    public static final int QUERY_NOT_FOUND = 22;

    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    public static final int FILTER_DATA_NOT_EXIST = 27;

    public static final int FILTER_DATA_NOT_LATEST = 28;

    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    public static final int TRANSACTION_STATE_UNKNOW = 202;

    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
    public static final int NO_BUYER_ID = 204;

    public static final int NOT_IN_CURRENT_UNIT = 205;

    public static final int CONSUMER_NOT_ONLINE = 206;

    public static final int CONSUME_MSG_TIMEOUT = 207;

    public static final int NO_MESSAGE = 208;

    public static final int UPDATE_AND_CREATE_ACL_CONFIG_FAILED = 209;

    public static final int DELETE_ACL_CONFIG_FAILED = 210;

    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED = 211;

    public static final int POLLING_FULL = 209;

    public static final int POLLING_TIMEOUT = 210;

    public static final int BROKER_NOT_EXIST = 211;

    public static final int BROKER_DISPATCH_NOT_COMPLETE = 212;

    public static final int BROADCAST_CONSUMPTION = 213;

    public static final int FLOW_CONTROL = 215;

    public static final int NOT_LEADER_FOR_QUEUE = 501;

    public static final int ILLEGAL_OPERATION = 604;

    public static final int RPC_UNKNOWN = -1000;
    public static final int RPC_ADDR_IS_NULL = -1002;
    public static final int RPC_SEND_TO_CHANNEL_FAILED = -1004;
    public static final int RPC_TIME_OUT = -1006;

    public static final int GO_AWAY = 1500;

    /**
     * Controller response code
     */
    public static final int CONTROLLER_FENCED_MASTER_EPOCH = 2000;
    public static final int CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH = 2001;
    public static final int CONTROLLER_INVALID_MASTER = 2002;
    public static final int CONTROLLER_INVALID_REPLICAS = 2003;
    public static final int CONTROLLER_MASTER_NOT_AVAILABLE = 2004;
    public static final int CONTROLLER_INVALID_REQUEST = 2005;
    public static final int CONTROLLER_BROKER_NOT_ALIVE = 2006;
    public static final int CONTROLLER_NOT_LEADER = 2007;

    public static final int CONTROLLER_BROKER_METADATA_NOT_EXIST = 2008;

    public static final int CONTROLLER_INVALID_CLEAN_BROKER_METADATA = 2009;

    public static final int CONTROLLER_BROKER_NEED_TO_BE_REGISTERED = 2010;

    public static final int CONTROLLER_MASTER_STILL_EXIST = 2011;

    public static final int CONTROLLER_ELECT_MASTER_FAILED = 2012;
    
    public static final int CONTROLLER_ALTER_SYNC_STATE_SET_FAILED = 2013;

    public static final int CONTROLLER_BROKER_ID_INVALID = 2014;

    public static final int CONTROLLER_JRAFT_INTERNAL_ERROR = 2015;

    public static final int CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS = 2016;
}
