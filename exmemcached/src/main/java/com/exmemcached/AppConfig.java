package com.exmemcached;

import com.google.code.ssm.config.DefaultAddressProvider;
import com.google.code.ssm.providers.xmemcached.MemcacheClientFactoryImpl;
import com.google.code.ssm.CacheFactory;
import com.google.code.ssm.providers.xmemcached.XMemcachedConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.*;
import java.util.HashSet;
import java.util.Set;
import org.springframework.cache.annotation.CachingConfigurerSupport;

import com.google.code.ssm.spring.SSMCache;
import com.google.code.ssm.spring.SSMCacheManager;

@Configuration
@EnableCaching(proxyTargetClass = true)
@EnableAspectJAutoProxy
@ImportResource("classpath:simplesm-context.xml")
public class AppConfig extends CachingConfigurerSupport {

    @Bean
    App app() {
        return new App();
    }

    @Bean
    @Override
    public CacheManager cacheManager() {
        Set<SSMCache> ssmCacheSet = new HashSet<>();
        SSMCache ssmCache = new SSMCache(cacheFactory().getCache(), 10, true);
        ssmCacheSet.add(ssmCache);
        SSMCacheManager ssmCacheManager = new SSMCacheManager();
        ssmCacheManager.setCaches(ssmCacheSet);
        return ssmCacheManager;
    }

    @Bean
    @DependsOn("cacheBase")
    public CacheFactory cacheFactory() {
        String server = "127.0.0.1:11211";
        String cacheName = "default";

        CacheFactory cacheFactory = new CacheFactory();
        cacheFactory.setCacheName(cacheName);
        cacheFactory.setCacheClientFactory(new MemcacheClientFactoryImpl());
        XMemcachedConfiguration cacheConfiguration = createCacheConfiguration(server);
        cacheFactory.setAddressProvider(new DefaultAddressProvider(server));
        cacheFactory.setConfiguration(cacheConfiguration);
        return cacheFactory;
    }

    private XMemcachedConfiguration createCacheConfiguration(final String server) {
        XMemcachedConfiguration cacheConfiguration = new XMemcachedConfiguration();
        cacheConfiguration.setConsistentHashing(true);
        cacheConfiguration.setUseBinaryProtocol(false);
        return cacheConfiguration;
    }
}
