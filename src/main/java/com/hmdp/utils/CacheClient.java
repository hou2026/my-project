package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component

public class CacheClient {
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    /**
     * 设置普通缓存（带TTL过期时间）
     * @param key 缓存键
     * @param value 缓存值
     * @param time 过期时间
     * @param unit 时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 设置逻辑过期缓存（解决缓存击穿问题）
     * @param key 缓存键
     * @param value 缓存值
     * @param time 逻辑过期时间
     * @param unit 时间单位
     */
    public void setLogicalExpier(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    /**
     * 查询缓存（解决缓存穿透问题）
     * @param keyPrefix 缓存键前缀
     * @param id 查询ID
     * @param type 返回类型
     * @param dbFallback 数据库查询回调函数
     * @param time 缓存过期时间
     * @param unit 时间单位
     * @return 查询结果
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        // 1. 查询Redis缓存
        String Json = stringRedisTemplate.opsForValue().get(keyPrefix + id);

        // 2. 缓存命中：直接反序列化返回
        if (StrUtil.isNotBlank(Json)) {
            return JSONUtil.toBean(Json, type);
        }

        // 3. 命中空值（防缓存穿透）：直接返回null
        if (Json != null && Json.isEmpty()) {
            return null;
        }

        // 4. 缓存未命中：查询数据库
        R r = dbFallback.apply(id);

        // 5. 数据库中不存在：写入空值缓存并设置较短TTL，防止大量请求穿透到数据库
        if (r == null) {
            stringRedisTemplate.opsForValue().set(keyPrefix + id, "", time, unit);
            return null;
        }

        // 6. 数据库中存在：写入Redis缓存，并设置随机过期时间防止缓存雪崩
        Random random = new Random();
        int offset = random.nextInt(2) + 1; // 随机增加1-2分钟
        stringRedisTemplate.opsForValue().set(keyPrefix + id, JSONUtil.toJsonStr(r),  time + offset, unit);

        return r;
    }

    /**
     * 查询缓存（基于逻辑过期解决缓存击穿问题）
     * @param keyPrefix 缓存键前缀
     * @param id 查询ID
     * @param type 返回类型
     * @param dbFallback 数据库查询回调函数
     * @param time 逻辑过期时间
     * @param unit 时间单位
     * @return 查询结果
     */
    public <R,ID> R queryWithLogicalexpier(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        // 1. 查询Redis缓存
        String Json = stringRedisTemplate.opsForValue().get(keyPrefix + id);

        // 2. 缓存未命中：直接返回null（逻辑过期方案通常要求热点数据预先加载）
        if (StrUtil.isBlank(Json)) {
            return null;
        }

        // 3. 缓存命中：将JSON反序列化为自定义的RedisData对象（包含数据和逻辑过期时间）
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);

        // 4. 提取缓存中的业务数据
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);

        // 5. 判断逻辑过期时间是否已到
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            // 未过期：直接返回旧数据
            return r;
        }

        // 6. 已过期：尝试获取互斥锁（使用独立的锁key避免与缓存key冲突）
        String lockKey = LOCK_SHOP_KEY + id;
        Boolean flag = tryLock(lockKey);
        if (!flag){
            // 获取锁失败：说明有其他线程正在重建缓存，直接返回旧数据（保证高可用）
            return r;
        }

        // 7. 获取锁成功：开启独立线程异步重建缓存
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            try {
                // 查询数据库
                R shop = dbFallback.apply(id);
                // 进行缓存重建
                this.setLogicalExpier(keyPrefix+id, shop, time, unit);
            } catch (Exception e) {
                e.printStackTrace(); // 记录异常，避免静默失败
            } finally {
                unLock(lockKey); // 确保释放锁
            }
        });

        // 8. 返回当前旧数据（不阻塞当前请求）
        return r;
    }

    /**
     * 尝试获取分布式锁
     * @param key 锁的键名
     * @return 是否获取成功
     */
    private Boolean tryLock(String key){
        // 使用SETNX实现互斥锁，并设置10秒超时防止死锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放分布式锁
     * @param key 锁的键名
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


}
