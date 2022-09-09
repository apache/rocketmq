package org.apache.rocketmq.logging.dynamic;

import java.io.Serializable;


public class LoggerBean implements Serializable {
    
    private static final long serialVersionUID = -5603077155885978439L;
    private String name;
    
    private String level;
    
    public LoggerBean() {
    }
    
    public LoggerBean(String name, String level) {
        this.name = name;
        this.level = level;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getLevel() {
        return level;
    }
    
    public void setLevel(String level) {
        this.level = level;
    }
    
    @Override
    public String toString() {
        return "{\"LoggerBean\":{" + "\"name\":\"" + name +
            "\"" + ",\"level\":\"" + level + "\"" + "}}";
    }
}
