package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    //记录开始时的时间戳
    private static final long BEGIN_TIMESTAMP=1767225600;
    private static final int COUNT_BITS=32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public  long nextId(String keyProfix) {

        //1.生成时间戳
        LocalDateTime now=LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyProfix + ":" + date);
        //3.拼接并返回
        return timestamp<<COUNT_BITS | count;
    }


}
