package com.hmdp.utils;


import com.hmdp.service.ILock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class simpleRedisLock implements ILock {
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String keyProfix="lock:";
    private static final String threadProfix= UUID.randomUUID().toString().replace("-","")+"-";
    
    // Lua脚本常量
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    
    // 静态代码块初始化Lua脚本
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("lua/unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public simpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String threadId = threadProfix+Thread.currentThread().getId();
        //尝试获取锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(keyProfix + name, threadId + "", timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(flag);
    }

//    @Override
//    public void unlock() {
//        //拿到当前锁标识
//        String threadId = threadProfix+Thread.currentThread().getId();
//        //获取数据库中的锁标识
//        String string = stringRedisTemplate.opsForValue().get(keyProfix + name);
//        //判断是否一致
//        if (threadId.equals(string)){
//            stringRedisTemplate.delete(keyProfix + name);
//        }
//    }
@Override
public  void unlock() {
   //使用lua脚本完成释放锁的功能
    String threadId = threadProfix + Thread.currentThread().getId();
    stringRedisTemplate.execute(
        UNLOCK_SCRIPT,
       Collections.singletonList(keyProfix + name),
        threadId
    );
}
}
