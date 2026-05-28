package com.hmdp.config;

import com.hmdp.utils.Interceptor;
import com.hmdp.utils.RefreshInterceptor;
import org.springframework.aop.framework.autoproxy.InfrastructureAdvisorAutoProxyCreator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    /**
     * 暴露 AOP 代理，使 AopContext.currentProxy() 可用
     */
    @Bean
    public InfrastructureAdvisorAutoProxyCreator infrastructureAdvisorAutoProxyCreator() {
        InfrastructureAdvisorAutoProxyCreator creator = new InfrastructureAdvisorAutoProxyCreator();
        creator.setExposeProxy(true);
        return creator;
    }
    
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new Interceptor()).excludePathPatterns(
                "/shop/**",
                "/voucher/**",
                "/shop-type/**",
                "/upload/**",
                "/blog/hot",
                "/user/code",
                "/user/login"
        ).order(1);
        registry.addInterceptor(new RefreshInterceptor(stringRedisTemplate)).order(0);
    }
}
