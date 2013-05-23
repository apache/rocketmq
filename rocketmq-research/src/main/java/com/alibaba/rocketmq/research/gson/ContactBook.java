package com.alibaba.rocketmq.research.gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.Expose;


public class ContactBook extends Convert {
    @Expose
    private String name;
    @Expose
    private List<Contact> contactList = new ArrayList<Contact>();
    @Expose
    private Map<String, String> customField = new HashMap<String, String>();

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


    public Map<String, String> getCustomField() {
        return customField;
    }


    public void setCustomField(Map<String, String> customField) {
        this.customField = customField;
    }


    public int getCode() {
        return code;
    }


    public void setCode(int code) {
        this.code = code;
    }


    @Override
    public String toString() {
        return "ContactBook [name=" + name + ", contactList=" + contactList + ", customField=" + customField
                + ", code=" + code + "]";
    }
}
