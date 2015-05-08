## Tajo-0.10.1 CDH Edition

* Last Update Date: 2015-05-08

#### Download source code

```sh
git clone -b CDH-5_3 https://github.com/hyunsik/tajo.git
```

#### How to build
```sh
cd tajo
mvn clean install package  -DskipTests -Pdist -Dhadoop.version=2.5.0-cdh5.3.3
```
