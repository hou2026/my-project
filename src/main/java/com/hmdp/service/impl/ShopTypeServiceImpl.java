package com.hmdp.service.impl;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result joinRedis() {
        String key = "shop:type:key";
        // 1. 先从Redis缓存中查询商铺类型数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断缓存是否存在且非空
        if (StrUtil.isNotBlank(json)){
            // 缓存命中：将JSON字符串反序列化为对象列表并返回
            List<ShopType> typeList = JSONUtil.toList(json, ShopType.class);
            return Result.ok(typeList);
        }
        // 3. 缓存未命中：从数据库查询并按sort字段升序排序
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4. 将查询结果序列化为JSON并存入Redis缓存
        String jsonStr = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(key,jsonStr,30, TimeUnit.MINUTES);
        // 5. 返回查询结果
        return Result.ok(typeList);

    }
}
