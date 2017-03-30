
# 프로젝트 생성


```
$ mvn archetype:generate -DgroupId=com.tmon -DartifactId=app -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
$ cd app 
$ mkdir src\main\resources
```

# 스프링 관련 파일 추가

+ /pom.xml
+ /src/main/java/com/tmon/HelloWorld.java
+ /src/main/resources/applicationContext.xml


# 빌드

```
$ mvn package
```

# 실행

```
$ mvn exec:java -Dexec.mainClass=com.tmon.App -Dexec.args="arg1 arg2 arg3"
```


