package com.alibaba.rocketmq.research.gson;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class TestFastjson {

    public static void main(String[] args) {
        ContactBook contactBook = new ContactBook();

        contactBook.setName("张三的通信录");
        contactBook.setCode(100);

        contactBook.getContactList().add(new Contact("刘德华", 54, 60.56, "本科\"", SexType.BOY));
        contactBook.getContactList().add(new Contact("张惠妹", 41, 52.69, "研究生", SexType.GIRL));
        contactBook.getContactList().add(new Contact("周星驰", 54, 61.22, "博士", SexType.BOY));

        // String json = JSON.toJSONString(contactBook,
        // SerializerFeature.WriteClassName);
        // System.out.println(json);

        // ContactBook fan = (ContactBook) JSON.parse(json);

        // ContactBook fan = JSON.parseObject(json, ContactBook.class);
        // System.out.println(fan);
    }
}
