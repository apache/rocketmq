package org.apache.rocketmq.broker.acl.domain;

/**
 * @author ycc
 * @date 2018/08/13
 */

public enum  OpsEnum {

    AUTH_READ(1,"读权限"),

    AUTH_WRITE(2,"写权限"),

    AUTH_UPDATE(3,"修改权限"),

    AUTH_DEL(4,"删除权限"),

    AUTH_CREATE(5,"添加操作者权限"),

    AUTH_OWNER(0,"所有者")
    ;
    Integer code;
    String msg;

    private OpsEnum(Integer code,String msg) {
        this.code = code;
        this.msg =msg;
    }

    public static OpsEnum getOpsEnum(Integer code) {
        if (code == null) {
            return null;
        }
        for (OpsEnum ops : OpsEnum.values()) {
            if (ops.code.equals(code)) {
                return ops;
            }
        }
        return null;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
