package org.apache.rocketmq.acl.dao.domain;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * Acldo
 * @author chujie.gcj
 * @date 2018-08-14
 */
public class AclDO implements Serializable {
    private static final long serialVersionUID = 1884828297962130471L;

    private Long id;
    private Date gmtCreate;
    private Date gmtModify;
    private Integer rowStatus;
    private Long rowVersion;

    private String topic;
    private String appName;
    private String userInfo;

    private Map<String,Object> attributeMap;

    public Long getId(){
        return this.id;
    }
    public void setId(Long id){
        this.id = id;
    }

    public Date getGmtCreate(){
        return this.gmtCreate;
    }
    public void setGmtCreate(Date gmtCreate){
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModify(){
        return this.gmtModify;
    }
    public void setGmtModify(Date gmtModify){
        this.gmtModify = gmtModify;
    }

    public Long getRowVersion(){
        return this.rowVersion;
    }
    public void setRowVersion(Long rowVersion){
        this.rowVersion = rowVersion;
    }

    public Integer getRowStatus(){
        return this.rowStatus;
    }
    public void setRowStatus(Integer rowStatus){
        this.rowStatus = rowStatus;
    }

    public String getTopic(){
        return this.topic;
    }
    public void setTopic(String topic){
        this.topic = topic;
    }

    public String getAppName(){
        return this.appName;
    }
    public void setAppName(String appName){
        this.appName = appName;
    }

    public String getUserInfo(){
        return this.userInfo;
    }
    public void setUserInfo(String userInfo){
        this.userInfo = userInfo;
    }

    public String getAttribute(){
        if(MapUtils.isEmpty(attributeMap)){
            return null ;
        }
        return new Gson().toJson(attributeMap);
    }
    public void setAttribute(String attribute){
        this.attributeMap = new Gson().fromJson(attribute,new TypeToken<Map<String,Object>>(){}.getType());
    }

    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}

