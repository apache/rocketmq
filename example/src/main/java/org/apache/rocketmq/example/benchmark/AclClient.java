package org.apache.rocketmq.example.benchmark;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

public class AclClient {

    private static final String ACL_ACCESS_KEY = "rocketmq2";

    private static final String ACL_SECRET_KEY = "12345678";

    static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(ACL_ACCESS_KEY,ACL_SECRET_KEY));
    }
}
