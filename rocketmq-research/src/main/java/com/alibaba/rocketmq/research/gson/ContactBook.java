package com.alibaba.rocketmq.research.gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ContactBook extends Convert {
    private String name;
    private List<Contact> contactList = new ArrayList<Contact>();
    private Map<Contact, String> customField = new HashMap<Contact, String>();

    private int code;


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public List<Contact> getContactList() {
        return contactList;
    }


    public void setContactList(List<Contact> contactList) {
        this.contactList = contactList;
    }


    public int getCode() {
        return code;
    }


    public void setCode(int code) {
        this.code = code;
    }


    public Map<Contact, String> getCustomField() {
        return customField;
    }


    public void setCustomField(Map<Contact, String> customField) {
        this.customField = customField;
    }


    @Override
    public String toString() {
        return "ContactBook [name=" + name + ", contactList=" + contactList + ", customField=" + customField
                + ", code=" + code + "]";
    }
}
