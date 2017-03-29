
https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html
http://www.mkyong.com/spring/quick-start-maven-spring-example/

```
$ mvn archetype:generate -DgroupId=com.mycompany.app -DartifactId=my-app -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
$ cd my-app 
$ mkdir src\main\resources
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


```
$ mvn package
$ mvn exec:java -Dexec.mainClass=com.mycompany.app.App -Dexec.args="arg1 arg2 arg3"
```

