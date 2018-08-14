package org.apache.rocketmq.broker.acl.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.acl.domain.AclDomain;
import org.apache.rocketmq.broker.acl.domain.OpsEnum;
import org.apache.rocketmq.common.TopicConfig;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AclDao {

    private static final String sysfilePath = "../../../../../../../../../";
    public AclDao() {
    }

    public static boolean add(AclDomain aclDomain){
        if(null == aclDomain)return false;
        String topicId = aclDomain.getTopicId();
        OpsEnum opsEnum = aclDomain.getType();
        if(StringUtils.isEmpty(topicId)|| null == opsEnum) return false;
        String key = opsEnum.getCode()+","+topicId;
        TopicConfig.aclMap.put(key,aclDomain);
        return writeLog(aclDomain,"add");
    }

    public static boolean update(AclDomain aclDomain){
        if(null == aclDomain)return false;
        String topicId = aclDomain.getTopicId();
        OpsEnum opsEnum = aclDomain.getType();
        if(StringUtils.isEmpty(topicId)|| null == opsEnum) return false;
        String key = opsEnum.getCode()+","+topicId;
        AclDomain tmp = TopicConfig.aclMap.get(key);
        if(null!=aclDomain.getGroupId()){
            tmp.setGroupId(aclDomain.getGroupId());
        }
        if(null != aclDomain.getUserIds()){
            tmp.setUserIds(aclDomain.getUserIds());
        }
        if(null!=aclDomain.getStatus()){
            tmp.setStatus(aclDomain.getStatus());
        }
        TopicConfig.aclMap.put(key,tmp);
        return writeLog(tmp,"update");
    }

/*    public static boolean deleteByTopicId(String topicId){

    }*/

    public static Set<String> getUserIDByTopicId(String topicId,OpsEnum opsEnum){
        Set<String> res = new HashSet<>();
        if(StringUtils.isEmpty(topicId)|| null == opsEnum) return res;
        String key = opsEnum.getCode()+","+topicId;
        res = TopicConfig.aclMap.get(key).getUserIds();
        return res;
    }

    private static boolean writeLog(AclDomain domain,String opsType){
//TODO
        String path = "";//读取配置文件 获取log存放位置
        //TODO
        //获取log文件，没有就创建
        File file = new File(path);

        //以追加的方式加入log文件末尾
        String content = opsType+":"+domain.toString();
        BufferedWriter out = null;
        try {
            if(!file.exists()){
                file.createNewFile();
            }
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)));
            out.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(out != null){
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static boolean recoverFromLog(){
        String path = sysfilePath+ "authLog.txt";//读取配置文件 获取log存放位置
        StringBuffer sb = new StringBuffer();
        String line = null;
        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            line = in.readLine();
            while(line!=null) {
                if (!StringUtils.isEmpty(line)) {
                    String[] opsStr = line.split(":");
                    switch (opsStr[0]) {
                        case "add":
                            add(getAclDomainFromString(opsStr[1]));
                            break;
                        case "update":
                            update(getAclDomainFromString(opsStr[1]));
                            break;
                        default:
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private static AclDomain getAclDomainFromString(String str){
        AclDomain aclDomain =  new AclDomain();
        if(StringUtils.isEmpty(str))return aclDomain;
        String[] strs = str.split(",");
        aclDomain.setTopicId(strs[0]);
        aclDomain.setGroupId(strs[1]);
        aclDomain.setType(OpsEnum.getOpsEnum(Integer.valueOf(strs[3])));
        aclDomain.setStatus(Integer.parseInt(strs[4]));
        String ids = strs[2].substring(1,strs[2].length()-1);
        Set<String> userIds = new HashSet<>();
        for(String id:ids.split(",")){
            userIds.add(id);
        }
        aclDomain.setUserIds(userIds);
        return aclDomain;
    }
}
