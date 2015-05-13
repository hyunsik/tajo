# Tajo-0.10.1 for CDH5

This branch maintains Tajo 0.10.1 branch for CDH 5.

## History
* 2015-05-08: Initial commit
* 2015-05-12: Modified some jars CLASSPATH and tested on CDH 5.4

## Direct Download
 * [tajo-0.10.1-cdh5_4] (https://www.dropbox.com/s/ftwm402e39cmws7/tajo-0.11.0-SNAPSHOT.tar.gz?dl=0)
 * [Patch] (https://www.dropbox.com/s/5wngftt3ke66u4t/Tajo_for_CDH_5.patch?dl=0)

## Building the source code

### for CentOS 6
```
yum groupinstall 'Development Tools'
yum install git
```

Download protobuf 2.5.0 from https://github.com/google/protobuf/releases
```
tar xjvf protobuf-2.5.0.tar.bz2
cd protobuf-2.5.0
./configure --prefix=/usr/local && make -j8
```

Download maven 3.x
```
wget https://archive.apache.org/dist/maven/binaries/apache-maven-3.2.2-bin.zip
unzip apache-maven-3.2.2-bin.zip
```
Please put ```maven/bin/``` directory to ```PATH``` shell environment variable.

### Install SUN JDK 7

Download ```jdk-7uXX-linux-x64.rpm``` from http://www.oracle.com/technetwork/java/javase/downloads/index.html

```sh
sudo alternatives --install /usr/bin/java java /usr/java/latest/jre/bin/java 200000
sudo alternatives --install /usr/bin/javaws javaws /usr/java/latest/jre/bin/javaws 200000
sudo alternatives --install /usr/bin/javac javac /usr/java/latest/bin/javac 200000
sudo alternatives --install /usr/bin/jar jar /usr/java/latest/bin/jar 200000
```

Then, you can change your JDK as follows:
```sh
update-alternatives --config java
```
### Download source code

```sh
git clone -b CDH-5_3 https://github.com/hyunsik/tajo.git
```

### Compiling the source code
```sh
cd tajo
mvn clean install package  -DskipTests -Pdist -Dtar -Dhadoop.version=2.6.0-cdh5.4.0
```

Please refer to CDH Hadoop version at [here]. (http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_vd_cdh5_maven_repo.html)

### Installation
```sh
cp -a tajo/tajo-dist/target/TAJO-0.11.0-SNAPSHOT path/to/install
```

## Configuration

Set ```JAVA_HOME``` in ```tajo/conf/tajo-env.sh``` as follows:
```
export JAVA_HOME=/usr/java/latest
```

Copy ```tajo/conf/tajo-site.xml.template``` to ```tajo/conf/tajo-site.xml``` and then remove the comments as follows:
```
<property>
  <name>tajo.rootdir</name>
  <value>hdfs://hostname:port/tajo</value>
</property>
```

Then, you should ensure the permission as follows:
```
hadoop dfs -mkdir /tajo
hadoop dfs -chown tajo:tajo /tajo

hadoop dfs -mkdir tajo:tajo /tmp/tajo-${username}
hadoop dfs -chown tajo:tajo /tmp/tajo-${username}
```

Instead of ```/tajo```, you should use the specified directory used in ```tajo.rootdir``` in ```tajo-site.xml```. Also, you must substitute ```${username}``` for your real username.

For more detail, please refer to [Tajo official documentation] (http://tajo.apache.org/docs/0.10.0/configuration.html).
