package org.apache.rocketmq.common.protocol.header.namesrv.controller;

public class GetMetaDataResponseHeader {
    private String controllerLeaderId;
    private String controllerLeaderAddress;

    public GetMetaDataResponseHeader(String controllerLeaderId, String controllerLeaderAddress) {
        this.controllerLeaderId = controllerLeaderId;
        this.controllerLeaderAddress = controllerLeaderAddress;
    }

    public String getControllerLeaderId() {
        return controllerLeaderId;
    }

    public void setControllerLeaderId(String controllerLeaderId) {
        this.controllerLeaderId = controllerLeaderId;
    }

    public String getControllerLeaderAddress() {
        return controllerLeaderAddress;
    }

    public void setControllerLeaderAddress(String controllerLeaderAddress) {
        this.controllerLeaderAddress = controllerLeaderAddress;
    }
}
