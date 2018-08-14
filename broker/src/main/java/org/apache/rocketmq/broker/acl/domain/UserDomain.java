package org.apache.rocketmq.broker.acl.domain;
/**
 *@author ycc
 *@date 2018/08/13
 */
public class UserDomain {
    private Long id;
    private String passWord;

    public UserDomain(Long id, String passWord) {
        this.id = id;
        this.passWord = passWord;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UserDomain) {
            UserDomain person= (UserDomain) obj;
            if (id.equals(person.getId()) && passWord.equals(person.getPassWord())) {
                return true;
            }
            return false;
        }
        return false;
    }
}

