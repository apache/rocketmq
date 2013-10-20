git pull

git checkout develop

rm -rf target
rm -f devenv

export JAVA_HOME=/opt/taobao/java
export PATH=/opt/taobao/mvn/bin:$JAVA_HOME/bin:$PATH
mvn -Dmaven.test.skip=true clean package install assembly:assembly -U

ln -s target/alibaba-rocketmq-3.0.0-final.dir/alibaba-rocketmq devenv
