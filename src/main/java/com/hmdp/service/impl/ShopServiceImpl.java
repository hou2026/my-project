package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    @Override
    public Result QueryById(Long id) {
        //缓存穿透
       // Shop shop= queryWithPassThrough(id);
        //缓存击穿
       // Shop shop = queryWithMutex(id);
        Shop shop = queryWithLogicalexpier(id);
        return Result.ok(shop);
    }
    /**
     * 店铺缓存查询逻辑（解决缓存穿透问题）
     * @param id 店铺ID
     * @return 店铺对象或null
     */
    public Shop queryWithPassThrough(Long id){
        // 1. 查询Redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        
        // 2. 缓存命中：直接反序列化返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        
        // 3. 命中空值（防缓存穿透）：直接返回null
        if (shopJson != null && shopJson.isEmpty()) {
            return null;
        }
        
        // 4. 缓存未命中：查询数据库
        Shop shop = getById(id);
        
        // 5. 数据库中不存在：写入空值缓存并设置较短TTL，防止大量请求穿透到数据库
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        
        // 6. 数据库中存在：写入Redis缓存，并设置随机过期时间防止缓存雪崩
        Random random = new Random();
        int offset = random.nextInt(2) + 1; // 随机增加1-2分钟
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + offset, TimeUnit.MINUTES);
        
        return shop;
    }
    /**
     * 基于互斥锁解决缓存击穿问题
     * @param id 店铺ID
     * @return 店铺对象或null
     */
    public Shop queryWithMutex(Long id){
        // 1. 查询Redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        
        // 2. 缓存命中：直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        
        // 3. 命中空值缓存：返回null
        if (shopJson != null && shopJson.isEmpty()) {
            return null;
        }
        
        // 4. 缓存未命中：尝试获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            
            // 5. 获取锁失败：休眠后重试查询
            if (!isLock) {
                Thread.sleep(100);
                return queryWithMutex(id);
            }
            
            // 6. 获取锁成功：查询数据库
            shop = getById(id);
            
            // 7. 数据库不存在：写入空值缓存（防穿透）
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            
            // 8. 数据库存在：写入Redis缓存（加随机TTL防雪崩）
            Random random = new Random();
            int offset = random.nextInt(2) + 1;
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + offset, TimeUnit.MINUTES);
            
        } catch (InterruptedException e) {
            throw new RuntimeException("查询店铺时发生异常", e);
        } finally {
            // 9. 释放互斥锁，保证并发安全
            unLock(lockKey);
        }
        
        return shop;
    }
    /**
     * 基于逻辑过期解决缓存击穿问题
     * @return 店铺对象或null
     */
    /**
     * 基于逻辑过期解决缓存击穿问题
     * @param id 店铺ID
     * @return 店铺对象
     */
    public Shop queryWithLogicalexpier(Long id){
        // 1. 查询Redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 2. 缓存未命中：直接返回null（逻辑过期方案通常要求热点数据预先加载）
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 3. 缓存命中：将JSON反序列化为自定义的RedisData对象（包含数据和逻辑过期时间）
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        
        // 4. 提取缓存中的业务数据
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        
        // 5. 判断逻辑过期时间是否已到
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            // 未过期：直接返回旧数据
            return shop;
        }

        // 6. 已过期：尝试获取互斥锁（使用独立的锁key避免与缓存key冲突）
        String lockKey = LOCK_SHOP_KEY + id;
        Boolean flag = tryLock(lockKey);
        if (!flag){
            // 获取锁失败：说明有其他线程正在重建缓存，直接返回旧数据（保证高可用）
            return shop;
        }

        // 7. 获取锁成功：开启独立线程异步重建缓存
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            try {
                this.save2Redis(id, 20L); // 重建缓存并设置新的逻辑过期时间
            } catch (Exception e) {
                e.printStackTrace(); // 记录异常日志，避免静默失败
            } finally {
                unLock(lockKey); // 确保重建完成后释放锁
            }
        });

        // 8. 返回当前旧数据（不阻塞当前请求）
        return shop;
    }
    @Transactional
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("商户id不能为空");
        }
        //1.更新数据库信息
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    /**
     * 根据商铺类型分页查询（支持附近商户排序）
     * @param typeId 商铺类型ID
     * @param current 页码
     * @param x 经度
     * @param y 纬度
     * @return 商铺列表
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. 判断是否需要按距离排序
        if (x == null || y == null){
            // 无需距离排序：直接按数据库分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        
        // 2. 计算分页参数：起始偏移量和查询数量
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = SystemConstants.DEFAULT_PAGE_SIZE;
        
        // 3. 查询Redis GEO：获取指定范围内的商户（按距离排序）
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> search = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),  // 用户坐标
                new Distance(5000),                 // 搜索半径：5公里
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                        .includeDistance()          // 包含距离信息
                        .limit(end)                 // 限制返回数量
        );
        
        // 4. 判断查询结果是否为空
        if (search == null) {
            return Result.ok();
        }
        
        // 5. 解析GEO结果：提取商户ID和距离
        List<Long> ids = new ArrayList<>();
        Map<String, Distance> distanceMap = new HashMap<>();
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = search.getContent();
        
        // 跳过已查询的历史数据，实现分页效果
        if (content.size() <= from) {
            return Result.ok();
        }
        
        content.stream().skip(from).forEach(geoResult -> {
            // 提取商户ID
            String shopId = geoResult.getContent().getName();
            ids.add(Long.valueOf(shopId));
            // 保存距离信息（后续用于填充到Shop对象）
            distanceMap.put(shopId, geoResult.getDistance());
        });
        
        // 6. 根据ID批量查询商户信息，并按GEO返回的顺序排序
        List<Shop> shops = query()
                .in("id", ids)
                .last("ORDER BY FIELD(id, " + StrUtil.join(",", ids) + ")")
                .list();
        
        // 7. 填充距离信息到每个商户对象
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        
        return Result.ok(shops);
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

    public void save2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //查询商户信息
        Shop shop = getById(id);
        Thread.sleep(200);
        //添加过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //加入redis（使用CACHE_SHOP_KEY而非LOCK_SHOP_KEY）
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
}
