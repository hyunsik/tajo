## Tajo-0.10.1 CDH Edition

* Last Update Date: 2015-05-08

### Preparing

#### for CentOS 6
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

### How to build
```sh
cd tajo
mvn clean install package  -DskipTests -Pdist -Dtar -Dhadoop.version=2.6.0-cdh5.4.0
```

Please refer to CDH Hadoop version at [here] (http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_vd_cdh5_maven_repo.html)

### Installation
```sh
cp -a tajo/tajo-dist/target/TAJO-0.11.0-SNAPSHOT path/to/install
```

### Configuration

Set ```JAVA_HOME``` in ```tajo/conf/tajo-env.sh``` as follows:
```
export JAVA_HOME=/usr/java/latest
```
