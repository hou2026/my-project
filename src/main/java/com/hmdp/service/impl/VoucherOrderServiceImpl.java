package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.config.RedissonConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import com.hmdp.utils.simpleRedisLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    // Lua 脚本返回值常量
    private static final int SECKILL_SUCCESS = 0;      // 秒杀成功
    private static final int SECKILL_STOCK_EMPTY = 1;  // 库存不足
    private static final int SECKILL_REPEAT_ORDER = 2; // 重复下单

    // 加载 Lua 脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;

    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTER= Executors.newSingleThreadExecutor();
    //代理对象，用于事务调用
    private IVoucherOrderService proxy;
    @PostConstruct
    private void init(){
        // 启动异步订单处理线程
        SECKILL_ORDER_EXECUTER.submit(new VoucherOrderHandle());
    }
    
//    /**
//     * 异步订单处理任务：从阻塞队列中获取订单并保存到数据库
//     */
//    private class VoucherOrderHandle implements Runnable{
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    // 1. 从阻塞队列中获取订单信息（队列空时会阻塞等待）
//                    VoucherOrder voucherOrder = queue.take();
//
//                    // 2. 创建订单到数据库
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }
    /**
     * 异步订单处理任务：从消息队列中获取订单并保存到数据库
     */
    private class VoucherOrderHandle implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    // 1. 从消息队列中获取订单信息（队列空时会阻塞等待）
                    List<MapRecord<String, Object, Object>> record = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().block(Duration.ofSeconds(2)).count(1),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    //2.判断集合是否为空
                    if (record==null||record.isEmpty()){
                        continue;
                    }
                    //3.解析订单信息（即拿到订单的具体信息）
                    MapRecord<String, Object, Object> record1 = record.get(0);
                    Map<Object, Object> value = record1.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4.完成下单
                    handleVoucherOrder(voucherOrder);
                    //5.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record1.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理 pending list 后继续循环，而不是递归调用
                    try {
                        handlePandingList();
                    } catch (Exception ex) {
                        log.error("处理 pending list 异常", ex);
                    }
                }
            }
        }
        private void handlePandingList() {
            while (true){
                try {
                    // 1. 从消息队列中获取订单信息（队列空时会阻塞等待）
                    List<MapRecord<String, Object, Object>> record = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    //2.判断集合是否为空
                    if (record==null||record.isEmpty()){
                        break;
                    }
                    //3.解析订单信息（即拿到订单的具体信息）
                    MapRecord<String, Object, Object> record1 = record.get(0);
                    Map<Object, Object> value = record1.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4.完成下单
                    handleVoucherOrder(voucherOrder);
                    //5.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record1.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    break; // 异常时退出循环，避免无限重试
                }
            }
        }
    }
    @Override
    public Result secKillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //生成订单 ID
        long orderId = redisIdWorker.nextId("order");
            
        // 执行 Lua 脚本进行秒杀判断和库存扣减（原子操作）
        Long result = stringRedisTemplate.execute(
            SECKILL_SCRIPT,
            Collections.emptyList(),
            voucherId.toString(),
            userId.toString(),
            String.valueOf(orderId)
        );
            
        // 解析 Lua 脚本返回值，防止 null 指针异常
        if (result == null) {
            return Result.fail("系统错误");
        }
        
        // 判断秒杀结果：0-成功，1-库存不足，2-重复下单
        int resultCode = result.intValue();
        if (resultCode != 0) {
            return Result.fail(resultCode == 1 ? "库存不足" : "不能重复下单");
        }
        //获取代理对象
         proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //为保证一人一单，需判断用户是否已存在订单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if(count > 0){
            return ;
        }
        
        //防止超卖
        boolean flag = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if(!flag){
            return ;
        }

        //将订单保存到数据库
        save(voucherOrder);
    }
    
    /**
     * 处理订单：将订单信息保存到数据库
     */
    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        
        // 使用分布式锁保证一人一单
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单，userId: {}", userId);
            return;
        }
        
        try {
            // 通过代理对象调用，确保事务生效
            proxy.createVoucherOrder(voucherOrder);
        } catch (Exception e) {
            log.error("创建订单失败", e);
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
}
