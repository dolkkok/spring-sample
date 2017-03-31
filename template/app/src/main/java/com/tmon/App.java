package com.tmon;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.MessageSource;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Properties;

public class App {

    public static void main(String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        HelloWorld obj = (HelloWorld) context.getBean("helloBean");
        obj.printHello();

        Properties prop = new Properties();
        prop.load(HelloWorld.class.getClassLoader().getResourceAsStream("data.properties"));
        System.out.println("Mode : " + prop.getProperty("mode"));
    }
}
