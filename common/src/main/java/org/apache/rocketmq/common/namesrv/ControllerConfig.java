package org.apache.rocketmq.common.namesrv;

import java.io.File;

public class ControllerConfig {

    /**
     * Is startup the controller in this name-srv
     */
    private boolean isStartupController = false;

    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int controllerThreadPoolNums = 16;

    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int controllerRequestThreadPoolQueueCapacity = 50000;

    private String controllerDLegerGroup;
    private String controllerDLegerPeers;
    private String controllerDLegerSelfId;
    private int mappedFileSize = 1024 * 1024 * 1024;
    private String controllerStorePath = System.getProperty("user.home") + File.separator + "DledgerController";

    /**
     * Whether the controller can elect a master which is not in the syncStateSet.
     */
    private boolean enableElectUncleanMaster = false;

    /**
     * Whether process read event
     */
    private boolean isProcessReadEvent = false;

    public boolean isStartupController() {
        return isStartupController;
    }

    public void setStartupController(boolean startupController) {
        isStartupController = startupController;
    }

    public int getControllerThreadPoolNums() {
        return controllerThreadPoolNums;
    }

    public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
        this.controllerThreadPoolNums = controllerThreadPoolNums;
    }

    public int getControllerRequestThreadPoolQueueCapacity() {
        return controllerRequestThreadPoolQueueCapacity;
    }

    public void setControllerRequestThreadPoolQueueCapacity(int controllerRequestThreadPoolQueueCapacity) {
        this.controllerRequestThreadPoolQueueCapacity = controllerRequestThreadPoolQueueCapacity;
    }

    public String getControllerDLegerGroup() {
        return controllerDLegerGroup;
    }

    public void setControllerDLegerGroup(String controllerDLegerGroup) {
        this.controllerDLegerGroup = controllerDLegerGroup;
    }

    public String getControllerDLegerPeers() {
        return controllerDLegerPeers;
    }

    public void setControllerDLegerPeers(String controllerDLegerPeers) {
        this.controllerDLegerPeers = controllerDLegerPeers;
    }

    public String getControllerDLegerSelfId() {
        return controllerDLegerSelfId;
    }

    public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
        this.controllerDLegerSelfId = controllerDLegerSelfId;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public void setMappedFileSize(int mappedFileSize) {
        this.mappedFileSize = mappedFileSize;
    }

    public String getControllerStorePath() {
        return controllerStorePath;
    }

    public void setControllerStorePath(String controllerStorePath) {
        this.controllerStorePath = controllerStorePath;
    }

    public boolean isEnableElectUncleanMaster() {
        return enableElectUncleanMaster;
    }

    public void setEnableElectUncleanMaster(boolean enableElectUncleanMaster) {
        this.enableElectUncleanMaster = enableElectUncleanMaster;
    }

    public boolean isProcessReadEvent() {
        return isProcessReadEvent;
    }

    public void setProcessReadEvent(boolean processReadEvent) {
        isProcessReadEvent = processReadEvent;
    }
}
