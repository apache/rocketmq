package com.alibaba.rocketmq.research.gson;

public class Contact {
    public final static String SUB_ALL = "*";
    private String name;
    private int age;
    private double weight;
    private String school;
    private SexType sex;


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public int getAge() {
        return age;
    }


    public void setAge(int age) {
        this.age = age;
    }


    public double getWeight() {
        return weight;
    }


    public void setWeight(double weight) {
        this.weight = weight;
    }


    public String getSchool() {
        return school;
    }


    public void setSchool(String school) {
        this.school = school;
    }


    public SexType getSex() {
        return sex;
    }


    public void setSex(SexType sex) {
        this.sex = sex;
    }


    public Contact() {
    }


    public Contact(String name, int age, double weight, String school, SexType sex) {
        super();
        this.name = name;
        this.age = age;
        this.weight = weight;
        this.school = school;
        this.sex = sex;
    }


    @Override
    public String toString() {
        return "Contact [name=" + name + ", age=" + age + ", weight=" + weight + ", school=" + school
                + ", sex=" + sex + "]";
    }
}
