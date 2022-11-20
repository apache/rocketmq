# TLS Configuration
This section introduce TLS configuration in RocketMQ.

## 1 Generate Certification Files
User can generate certification files using OpenSSL. Suggested to generate files in Linux.

### 1.1 Generate ca.pem
```shell
openssl req -newkey rsa:2048 -keyout ca_rsa_private.pem -x509 -days 365 -out ca.pem
```
### 1.2 Generate server.csr
```shell
openssl req -newkey rsa:2048 -keyout server_rsa.key  -out server.csr
```
### 1.3 Generate server.pem
```shell
openssl x509 -req -days 365 -in server.csr -CA ca.pem -CAkey ca_rsa_private.pem -CAcreateserial -out server.pem
```
### 1.4 Generate client.csr
```shell
openssl req -newkey rsa:2048 -keyout client_rsa.key -out client.csr
```
### 1.5 Generate client.pem
```shell
openssl x509 -req -days 365 -in client.csr -CA ca.pem -CAkey ca_rsa_private.pem -CAcreateserial -out client.pem
```
### 1.6 Generate server.key
```shell
openssl pkcs8 -topk8 -v1 PBE-SHA1-RC4-128 -in  server_rsa.key -out server.key
```
### 1.7 Generate client.key
```shell
openssl pkcs8 -topk8 -v1 PBE-SHA1-RC4-128 -in client_rsa.key -out client.key
```

## 2 Create tls.properties
Create tls.properties, correctly configure the path and password of the generated certificates.

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

If you need to authenticate the client connection, you also need to add the following content to the file.

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


## 3 Update Rocketmq JVM parameters

Edit the configuration file under the rocketmq/bin path to make tls.properties configurations take effect.

The value of "tls.config.file" needs to be replaced by the file path created in step 2.

### 3.1 Edit runserver.sh
Add following content in JAVA_OPT:
```shell
JAVA_OPT="${JAVA_OPT} -Dtls.server.mode=enforcing -Dtls.config.file=/opt/rocketmq-4.9.3/conf/tls.properties"
```

### 3.2 Edit runbroker.sh
Add following content in JAVA_OPT:

```shell
JAVA_OPT="${JAVA_OPT} -Dorg.apache.rocketmq.remoting.ssl.mode=enforcing -Dtls.config.file=/opt/rocketmq-4.9.3/conf/tls.properties  -Dtls.enable=true"
```

# 4 Client connection

Create tlsclient.properties using by client. Add following content:
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

Add following parameters in JVM. The value of "tls.config.file" needs to be replaced by the file path we created:
```properties
-Dtls.client.authServer=true -Dtls.enable=true  -Dtls.test.mode.enable=false  -Dtls.config.file=/opt/certs/tlsclient.properties
```

Enable TLS for client linke following:
```Java
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
