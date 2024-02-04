# TLS配置
本节介绍TLS相关配置

## 1 生成证书
开发、测试的证书可以自行安装OpenSSL进行生成.建议在Linux环境下安装Open SSL并进行证书生成。

### 1.1 生成ca.pem
```shell
openssl req -newkey rsa:2048 -keyout ca_rsa_private.pem -x509 -days 365 -out ca.pem
```
### 1.2 生成server.csr
```shell
openssl req -newkey rsa:2048 -keyout server_rsa.key  -out server.csr
```
### 1.3 生成server.pem
```shell
openssl x509 -req -days 365 -in server.csr -CA ca.pem -CAkey ca_rsa_private.pem -CAcreateserial -out server.pem
```
### 1.4 生成client.csr
```shell
openssl req -newkey rsa:2048 -keyout client_rsa.key -out client.csr
```
### 1.5 生成client.pem
```shell
openssl x509 -req -days 365 -in client.csr -CA ca.pem -CAkey ca_rsa_private.pem -CAcreateserial -out client.pem
```
### 1.6 生成server.key
```shell
openssl pkcs8 -topk8 -v1 PBE-SHA1-RC4-128 -in  server_rsa.key -out server.key
```
### 1.7 生成client.key
```shell
openssl pkcs8 -topk8 -v1 PBE-SHA1-RC4-128 -in client_rsa.key -out client.key
```

## 2 创建tls.properties
创建tls.properties文件，并将生成证书的路径和密码进行正确的配置.


```properties
# The flag to determine whether use test mode when initialize TLS context. default is true
tls.test.mode.enable=false                     
# Indicates how SSL engine respect to client authentication, default is none
tls.server.need.client.auth=require   
# The store path of server-side private key
tls.server.keyPath=/opt/certFiles/server.key
# The password of the server-side private key
tls.server.keyPassword=123456
# The store path of server-side X.509 certificate chain in PEM format
tls.server.certPath=/opt/certFiles/server.pem
# To determine whether verify the client endpoint's certificate strictly. default is false
tls.server.authClient=false
# The store path of trusted certificates for verifying the client endpoint's certificate
tls.server.trustCertPath=/opt/certFiles/ca.pem
```

如果需要客户端连接时也进行认证，则还需要在该文件中增加以下内容
```properties
# The store path of client-side private key 
tls.client.keyPath=/opt/certFiles/client.key
# The password of the client-side private key
tls.client.keyPassword=123456
# The store path of client-side X.509 certificate chain in PEM format
tls.client.certPath=/opt/certFiles/client.pem
# To determine whether verify the server endpoint's certificate strictly
tls.client.authServer=false                    
# The store path of trusted certificates for verifying the server endpoint's certificate
tls.client.trustCertPath=/opt/certFiles/ca.pem
```


## 3 配置Rocketmq启动参数

编辑rocketmq/bin路径下的配置文件，使tls.properties配置生效.-Dtls.config.file的值需要替换为步骤2中创建的tls.peoperties文件的路径

### 3.1 编辑runserver.sh，在JAVA_OPT中增加以下内容：
```shell
JAVA_OPT="${JAVA_OPT} -Dtls.server.mode=enforcing -Dtls.config.file=/opt/rocketmq-4.9.3/conf/tls.properties"
```

### 3.2 编辑runbroker.sh，在JAVA_OPT中增加以下内容：

```shell
JAVA_OPT="${JAVA_OPT} -Dorg.apache.rocketmq.remoting.ssl.mode=enforcing -Dtls.config.file=/opt/rocketmq-4.9.3/conf/tls.properties  -Dtls.enable=true"
```

# 4 客户端连接

创建客户端使用的tlsclient.properties，并加入以下内容：
```properties
# The store path of client-side private key 
tls.client.keyPath=/opt/certFiles/client.key
# The password of the client-side private key
tls.client.keyPassword=123456
# The store path of client-side X.509 certificate chain in PEM format
tls.client.certPath=/opt/certFiles/client.pem               
# The store path of trusted certificates for verifying the server endpoint's certificate
tls.client.trustCertPath=/opt/certFiles/ca.pem
```

JVM中需要加以下参数.tls.config.file的值需要使用之前创建的文件：
```shell
-Dtls.client.authServer=true -Dtls.enable=true  -Dtls.test.mode.enable=false  -Dtls.config.file=/opt/certs/tlsclient.properties
```

在客户端连接的代码中，需要将setUseTLS设置为true：
```java
public class ExampleProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        //setUseTLS should be true
        producer.setUseTLS(true);
        producer.start();

        // Send messages as usual.
        producer.shutdown();
    }    
}
```