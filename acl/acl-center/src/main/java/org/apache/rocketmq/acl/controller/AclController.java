package org.apache.rocketmq.acl.controller;


import org.apache.rocketmq.acl.AclService;
import org.apache.rocketmq.acl.service.AclServiceImpl;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("api/acl")
public class AclController {
    private AclService aclService ;

    @RequestMapping(method = RequestMethod.GET, value = "/aclCheck/{topic}/{appName}")
    @ResponseBody
    public Boolean aclCheck(@PathVariable String topic, @PathVariable String appName) throws IOException {
        aclService = new AclServiceImpl();
        return aclService.aclCheck(topic, appName);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/aclCheck/{topic}/{appName}/{userInfo}")
    @ResponseBody
    public Boolean aclCheck(@PathVariable String topic, @PathVariable String appName,@PathVariable String userInfo)
            throws IOException {
        aclService = new AclServiceImpl();
        return aclService.aclCheck(topic, appName,userInfo);
    }
}
