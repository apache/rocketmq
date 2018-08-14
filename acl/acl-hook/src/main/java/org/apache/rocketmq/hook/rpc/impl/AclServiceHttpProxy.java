package org.apache.rocketmq.hook.rpc.impl;

import org.apache.rocketmq.hook.rpc.AclService;

public class AclServiceHttpProxy implements AclService {

    private String host;
    private int port;
    OkHttpClient client = new OkHttpClient();

    private static Object innerLock = new Object();
    private static  AclServiceHttpProxy instance;

    public static AclService getInstance(String host, int port) {
        if (instance == null) {
            synchronized (innerLock) {
                if (instance == null) {
                    instance = new AclServiceHttpProxy(host, port);
                }
            }
        }
        return instance;
    }

    private AclServiceHttpProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }


    @Override
    public boolean aclCheck(String topic, String appName) {
        String url = "http://"+host+":"+port +"/api/acl/checkacl/"+topic+"/"+appName;
        Request request = new Request.Builder().url(url).build();
        Response response = client.newCall(request).execute();
        if (response.isSuccessful()) {
            return response.body().string();
        } else {
            throw new IOException("Unexpected code " + response);
        }
    }

    @Override
    public boolean aclCheck(String topic, String appName, String userInfo) {
        // todo
    }
}
