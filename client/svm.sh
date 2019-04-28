#!/bin/bash
SVM_OPT="--allow-incomplete-classpath --report-unsupported-elements-at-runtime"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.util.BouncyCastleSelfSignedCertGenerator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkNpnApplicationProtocolNegotiator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkNpnSslEngine"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkAlpnSslEngine"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.util.internal.JavassistTypeParameterMatcherGenerator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=com.alibaba.fastjson.serializer.JodaCodec"
#SVM_OPT="${SVM_OPT}  -H:+PrintClassInitialization"

SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=io.netty.handler.ssl.util.SelfSignedCertificate"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=io.netty.handler.ssl.util.ThreadLocalInsecureRandom"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=com.alibaba.fastjson.serializer.SerializeConfig"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=com.alibaba.fastjson.parser.ParserConfig"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=org.apache.rocketmq.client.impl.MQClientAPIImpl"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=com.alibaba.fastjson.serializer.SerializerFeature"

SVM_OPT="${SVM_OPT} --enable-url-protocols=http"

WORKDIR=`pwd`
CONFIG_OPT="-H:ConfigurationFileDirectories=${WORKDIR}/config  -H:+ReportExceptionStackTraces --allow-incomplete-classpath"

#Disable unsafe usage in netty. This option is provided by netty, not an univeral solution. A more general way
#is to use Graal's substition mechenism (see "Unsafe memory access" in 
#https://medium.com/graalvm/instant-netty-startup-using-graalvm-native-image-generation-ed6f14ff7692)
CONFIG_OPT="${CONFIG_OPT} -Dio.netty.noUnsafe=true"
#Compile to a SO file
CONFIG_OPT="${CONFIG_OPT} --shared -H:Name=rocketMQClient"
#Specify where is the C library file which defines the data structure used in exposed API. 
CONFIG_OPT="${CONFIG_OPT} -H:CLibraryPath=native"

#CONFIG_OPT="${CONFIG_OPT} -cp dynclass"

#Set your own $native_image enviroment variable which should refer to the bin\native-image file in your graalvm JDK. 
$native_image  $CONFIG_OPT $SVM_OPT  -jar target/rocketmq-client-4.5.1-SNAPSHOT-jar-with-dependencies.jar $1
