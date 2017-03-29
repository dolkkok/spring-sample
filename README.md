
https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html

http://www.mkyong.com/spring/quick-start-maven-spring-example/


```
$ mvn archetype:generate -DgroupId=com.mycompany.app -DartifactId=my-app -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
$ cd my-app 
$ mkdir src\main\resources
```

my-app/pom.xml
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany.app</groupId>
    <artifactId>my-app</artifactId>
    <packaging>jar</packaging>
    <version>1.0-SNAPSHOT</version>
    <name>my-app</name>
    <url>http://maven.apache.org</url>

    <properties>
        <spring.version>4.0.1.RELEASE</spring.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>3.8.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-core</artifactId>
            <version>${spring.version}</version>
        </dependency>

        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-web</artifactId>
            <version>${spring.version}</version>
        </dependency>

        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-webmvc</artifactId>
            <version>${spring.version}</version>
        </dependency>
    </dependencies>
</project>
```

src/main/resourcs/Spring-Module.xml
```xml
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.springframework.org/schema/beans
	http://www.springframework.org/schema/beans/spring-beans.xsd">

	<bean id="helloBean" class="com.mycompany.app.HelloWorld">
		<property name="name" value="Mkyong" />
	</bean>
</beans>
```


src/main/java/com/mycompany/app/HelloWorld.java
```java
package com.mycompany.app;

/**
 * Spring bean
 *
 */
public class HelloWorld {
	private String name;

	public void setName(String name) {
		this.name = name;
	}

	public void printHello() {
		System.out.println("Hello ! " + name);
	}
}
```

src/main/java/com/mycompany/app/App.java
```java
package com.mycompany.app;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        for (String s : args) {
            System.out.println(s);
        }
        
        System.out.println( "Hello World!" );
       
        ApplicationContext context = new ClassPathXmlApplicationContext("Spring-Module.xml");
		HelloWorld obj = (HelloWorld) context.getBean("helloBean");
		obj.printHello();
    }
}
```

```
$ mvn package
$ mvn exec:java -Dexec.mainClass=com.mycompany.app.App -Dexec.args="arg1 arg2 arg3"
```

