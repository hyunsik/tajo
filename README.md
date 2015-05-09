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


### Download source code

```sh
git clone -b CDH-5_3 https://github.com/hyunsik/tajo.git
```

### How to build
```sh
cd tajo
mvn clean install package  -DskipTests -Pdist -Dhadoop.version=2.5.0-cdh5.3.3
```
