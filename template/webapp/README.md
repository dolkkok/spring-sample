
# 프로젝트 생성


```
$ mvn archetype:generate -DgroupId=com.tmon -DartifactId=app -DarchetypeArtifactId=maven-archetype-webapp -DinteractiveMode=false
$ cd app 
$ mkdir src\main\resources
```

# 스프링 관련 파일 추가

+ /pom.xml
+ /src/main/java/com/tmon/MainController.java
+ /src/main/java/com/tmon/SessionFilter.java
+ /src/main/resources/log4j.properties
+ /src/main/resources/spring/dispatcher-servlet.xml
+ /src/main/webapp/jsp/admin.jsp
+ /src/main/webapp/jsp/greeting.jsp
+ /src/main/webapp/jsp/home.jsp
+ /src/main/webapp/jsp/list.jsp
+ /src/main/webapp/jsp/welcome.jsp
+ /src/main/webapp/WEB-INF/pages/login.html
+ /src/main/webapp/WEB-INF/pages/login_error.html
+ /src/main/webapp/WEB-INF/applicationContext.xml
+ /src/main/webapp/WEB-INF/web.xml


# 빌드

```
$ mvn package
```


# Tomcat Stop

```
$ "%CATALINA_HOME%\bin\catalina.bat" stop
```


# war 배포

```
$ copy /y target\webapp.war "%CATALINA_HOME%\webapps\"
```


# Tomcat Start

```
$ "%CATALINA_HOME%\bin\catalina.bat" start
```


# Tomcat Start Debugging Mode

```
$ "%CATALINA_HOME%\bin\catalina.bat" jpda start
```

