package com.hmdp;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private IShopService shopService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedisIdWorker redisIdWorker;
    private ExecutorService executorService = Executors.newFixedThreadPool(500);
    @Test
    public void idTest(){
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        for (int i = 0; i < 300; i++) {
            executorService.submit(task);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void loadShopTypeId(){
        // 1. 查询数据库中所有商铺信息
        List<Shop> list = shopService.list();
        
        // 2. 按商铺类型ID分组，得到 Map<typeId, List<Shop>>
        Map<Long,List<Shop>> map=list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        
        // 3. 遍历每个类型，将商铺坐标信息写入Redis GEO
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> shops = entry.getValue();
            
            // 构造Redis GEO Key: shop:geo:{typeId}
            String key=SHOP_GEO_KEY+typeId;
            
            // 收集当前类型下所有商铺的地理位置信息
            List<RedisGeoCommands.GeoLocation<String>> geoLocationList=new ArrayList();
            for (Shop shop : shops) {
                // 将商铺ID作为member，经纬度作为坐标，封装为GeoLocation对象
                geoLocationList.add(new RedisGeoCommands.GeoLocation<>(
                    shop.getId().toString(),
                    new Point(shop.getX(), shop.getY())
                ));
            }
            
            // 批量写入Redis GEO（使用GEOADD命令）
            stringRedisTemplate.opsForGeo().add(key, geoLocationList);
        }
    }
}
