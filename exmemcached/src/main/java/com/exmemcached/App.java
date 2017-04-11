package com.exmemcached;

import com.google.code.ssm.api.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Service;
import org.apache.log4j.Logger;

@Service
public class App {
    final String MEMCACHE_NAMESPACE = "MyCacheSpace";
    public static final Logger logger = Logger.getLogger(App.class);

    @ReadThroughSingleCache(namespace = MEMCACHE_NAMESPACE, expiration = 1000)
    public SomeData getSomeData(@ParameterValueKeyProvider long id) throws Exception {
        logger.info("캐쉬에 추가됨 ID : " + id);
        return new SomeData(id, "james");
    }

    @InvalidateSingleCache(namespace = MEMCACHE_NAMESPACE)
    public void removeCache(@ParameterValueKeyProvider long id) {
        logger.info("캐쉬가 제거됨 ID : " + id);
    }

    @UpdateSingleCache(namespace = MEMCACHE_NAMESPACE, expiration = 10)
    public void updateCache(@ParameterValueKeyProvider long id,
                                   @ParameterDataUpdateContent final SomeData overrideData) {
        logger.info("캐쉬가 업데이트 됨 ID : " + id);
    }

    public static void main( String[] args ) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        App app = context.getBean(App.class);

        /// #1. 캐쉬 읽기
        SomeData data1 = app.getSomeData(1);
        SomeData data2 = app.getSomeData(1);

        /// #2. 캐쉬 제거
        app.removeCache(1);

        /// #3. 캐쉬 추가
        SomeData data3 = app.getSomeData(1);

        /// #4. 캐쉬 갱신
        app.updateCache(1, new SomeData(1, "james dean"));

        /// #5 캐쉬 갱신 확인
        SomeData data4 = app.getSomeData(1);
        logger.info(data4.getName());

        /// #6 캐쉬 expire
        Thread.sleep(10000);
        SomeData data5 = app.getSomeData(1);
        logger.info(data5.getName());
    }
}
